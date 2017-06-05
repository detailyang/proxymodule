package redisproxy

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	ConnReadBufferSize    = 1024
	ConnWriteBufferSize   = 1024
	ConnKeepaliveInterval = 90
)

var (
	ErrEmptyCommand     = errors.New("empty command")
	ErrNotSupported     = errors.New("command not supported")
	ErrNotAuthenticated = errors.New("auth failed")

	errReadRequest = errors.New("invalid request protocol")
	errClientQuit  = errors.New("remote client quit")
)

type ResponseWriter interface {
	WriteError(error) error
	WriteString(string) error
	WriteInteger(int64) error
	WriteBulk([]byte) error
	WriteArray([]interface{}) error
	WriteSliceArray([][]byte) error
	WriteRawBytes(buf []byte) error
	Flush() error
}

type Client struct {
	RegCmds    *CmdRouter
	remoteAddr string
	cmd        string
	Args       [][]byte

	isAuthed bool
	resp     ResponseWriter
	buf      bytes.Buffer

	closeCh    chan struct{}
	closedFlag int32

	proxyStatistics ProxyStatisticsModule
}

func newClient() *Client {
	c := &Client{
		closeCh: make(chan struct{}),
	}
	c.isAuthed = false

	return c
}

func (c *Client) closed() bool {
	return atomic.LoadInt32(&c.closedFlag) >= 1
}

func (c *Client) close() {
	if atomic.CompareAndSwapInt32(&c.closedFlag, 0, 1) {
		close(c.closeCh)
	}
}

//should be called before reuse the client
func (c *Client) reset() {
	c.isAuthed = false
	c.cmd = ""
	c.buf.Reset()
	c.remoteAddr = ""
	c.proxyStatistics = nil
	c.Args = nil
	c.RegCmds = nil

	if c.closed() {
		c.closeCh = make(chan struct{})
		atomic.StoreInt32(&c.closedFlag, 0)
	}
}

func (c *Client) authEnabled() bool {
	return false
	//return len(c.app.cfg.AuthPassword) > 0
}

func (c *Client) perform() {
	var err error

	start := time.Now()

	if len(c.cmd) == 0 {
		err = ErrEmptyCommand
	} else if c.cmd == "ping" {
		if len(c.Args) == 0 {
			c.resp.WriteString("PONG")
		} else {
			c.resp.WriteBulk(c.Args[0])
		}
	} else if c.cmd == "quit" {
		c.resp.WriteString("OK")
		c.close()
	} else if exeCmd, ok := c.RegCmds.GetCmdHandler(c.cmd); !ok {
		err = ErrNotSupported
	} else if c.authEnabled() && !c.isAuthed && c.cmd != "auth" {
		err = ErrNotAuthenticated
	} else {
		//redisLog.Infof("redis command: %v with params: %v", string(c.cmd), c.Args)
		err = exeCmd(c, c.resp)
	}

	duration := time.Since(start)

	if redisLog.Level() > 1 || duration > time.Millisecond*100 {

		fullCmd := c.catGenericCommand()

		cost := duration.Nanoseconds() / 1000000

		truncateLen := len(fullCmd)
		if truncateLen > 256 {
			truncateLen = 256
		}

		redisLog.Infof("command from client: %v cost %v, command detail: %v, %v.", c.remoteAddr, cost,
			string(fullCmd[:truncateLen]), err)
	}

	if c.proxyStatistics != nil {
		if duration > time.Millisecond*2 {
			c.proxyStatistics.IncrSlowOperation(duration)
		}
		c.proxyStatistics.IncrOpTime(duration.Nanoseconds())
	}

	if err != nil {
		if c.proxyStatistics != nil {
			c.proxyStatistics.IncrFailedOperation()
		}
		c.resp.WriteError(err)

		fullCmd := c.catGenericCommand()

		truncateLen := len(fullCmd)
		if truncateLen > 256 {
			truncateLen = 256
		}

		redisLog.Infof("command execute failed, detail: %s, err: %s", string(fullCmd[:truncateLen]), err.Error())
	}

	c.resp.Flush()

	return
}

func (c *Client) catGenericCommand() []byte {
	buffer := c.buf
	buffer.Reset()

	buffer.Write([]byte(c.cmd))

	for _, arg := range c.Args {
		buffer.WriteByte(' ')
		buffer.Write(arg)
	}

	return buffer.Bytes()
}

func WriteValue(w ResponseWriter, value interface{}) {
	switch v := value.(type) {
	case []interface{}:
		w.WriteArray(v)
	case [][]byte:
		w.WriteSliceArray(v)
	case []byte:
		w.WriteBulk(v)
	case string:
		w.WriteString(v)
	case nil:
		w.WriteBulk(nil)
	case int64:
		w.WriteInteger(v)
	default:
		panic("invalid value type")
	}
}

type RespClient struct {
	*Client
	conn       net.Conn
	respReader *RespReader
	quit       <-chan bool
}

type BufRespWriter struct {
	buff *bufio.Writer
}

func NewEmptyClientRESP(quit <-chan bool) *RespClient {
	c := &RespClient{}
	c.Client = newClient()
	c.quit = quit
	return c
}

func NewClientRESP(conn net.Conn, quit <-chan bool) *RespClient {
	c := &RespClient{}

	c.Client = newClient()
	c.conn = conn
	c.quit = quit

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetReadBuffer(ConnReadBufferSize)
		tcpConn.SetWriteBuffer(ConnWriteBufferSize)
	}

	br := bufio.NewReaderSize(conn, ConnReadBufferSize)
	c.respReader = NewRespReader(br)

	c.resp = NewRespWriter(bufio.NewWriterSize(conn, ConnWriteBufferSize))
	c.remoteAddr = conn.RemoteAddr().String()
	if redisLog.Level() > 1 {
		redisLog.Infof("new redis client: %v", c.remoteAddr)
	}
	return c
}

func (c *RespClient) Reset(conn net.Conn) {
	c.conn = conn
	c.isAuthed = false
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetReadBuffer(ConnReadBufferSize)
		tcpConn.SetWriteBuffer(ConnWriteBufferSize)
	}

	if c.respReader == nil {
		br := bufio.NewReaderSize(conn, ConnReadBufferSize)
		c.respReader = NewRespReader(br)
	} else {
		c.respReader.br.Reset(conn)
	}
	if c.resp == nil {
		c.resp = NewRespWriter(bufio.NewWriterSize(conn, ConnWriteBufferSize))
	} else {
		c.resp.(*RespWriter).bw.Reset(conn)
	}
	c.remoteAddr = conn.RemoteAddr().String()

	if redisLog.Level() > 2 {
		redisLog.Infof("redis client reused: %v", c.remoteAddr)
	}
}

func (c *RespClient) Run() {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]

			redisLog.Errorf("client run panic %s:%v", buf, e)
		}
		if redisLog.Level() > 1 {
			redisLog.Infof("redis client closed: %v", c.remoteAddr)
		}
		c.Client.close()
		c.conn.Close()
	}()

	kc := time.Duration(ConnKeepaliveInterval) * time.Second
	for {
		select {
		case <-c.quit:
			return

		case <-c.closeCh:
			redisLog.Infof("remote client:%s ask to quit", c.conn.RemoteAddr().String())
			return

		default:
			if kc > 0 {
				c.conn.SetReadDeadline(time.Now().Add(kc))
			}

			c.cmd = ""
			c.Args = nil

			reqData, err := c.respReader.ParseRequest()
			if err == nil {
				err = c.handleRequest(reqData)
			} else if err == io.EOF {
				redisLog.Infof("encounter EOF while handling request, connection has been closed, remote:%s", c.remoteAddr)
				return
			} else if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				redisLog.Infof("encounter IO time-out while handling request, connection will been closed, remote:%s", c.remoteAddr)
				return
			} else if operr, ok := err.(*net.OpError); ok && operr.Err.Error() == syscall.ECONNRESET.Error() {
				redisLog.Infof("connection reset by peer while handling request, remote:%s", c.remoteAddr)
				return
			} else {
				redisLog.Errorf("handle request failed as err:%s, connection will be closed by server", err.Error())
				return
			}
		}
	}
}

func (c *RespClient) handleRequest(reqData [][]byte) error {
	if len(reqData) == 0 {
		c.cmd = ""
		c.Args = reqData[0:0]
	} else {
		c.cmd = strings.ToLower(string(reqData[0]))
		c.Args = reqData[1:]
	}

	c.perform()
	return nil
}
