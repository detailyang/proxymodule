package redisproxy

import (
	"fmt"
	"strings"
)

type CommandFunc func(c *Client, w ResponseWriter) error

type CmdRouter struct {
	cmds map[string]CommandFunc
}

func NewCmdRouter() *CmdRouter {
	return &CmdRouter{
		cmds: make(map[string]CommandFunc),
	}
}

func (r *CmdRouter) Register(name string, f CommandFunc) error {
	if _, ok := r.cmds[strings.ToLower(name)]; ok {
		return fmt.Errorf("%s has been registered", name)
	}
	r.cmds[name] = f
	return nil
}

func (r *CmdRouter) GetCmdHandler(name string) (CommandFunc, bool) {
	v, ok := r.cmds[strings.ToLower(name)]
	return v, ok
}

func (r *CmdRouter) Unregister(name string) {
	delete(r.cmds, name)
}
