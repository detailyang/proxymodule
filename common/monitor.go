package common

import (
	"os"
	"time"
)

var (
	hostName = "unknown host"
)

func init() {
	if name, err := os.Hostname(); err == nil {
		hostName = name
	}
}

/*
the monitor data format:
  [
  {
	  "business":"tether.kvproxy",
	  "timestamp":1456387601,
	  "metrics":{
		  "get":78219,
		  "set":21763,
		  "del":21625
	  },
	  "tags":{
		  "application":"kvstore",
		  "host":"",
		  "category":"",
	  },
  }
  ]
*/

type MonitorData struct {
	Business  string            `json:"business"`
	Timestamp int64             `json:"timestamp"`
	Metrics   map[string]uint64 `json:"metrics"`
	Tags      map[string]string `json:"tags"`
}

func NewMonitorData(app string, business string) *MonitorData {
	data := &MonitorData{
		Metrics: make(map[string]uint64),
		Tags:    make(map[string]string),
	}

	data.Business = business
	data.Tags["application"] = app
	data.Tags["host"] = hostName

	data.Timestamp = time.Now().Unix()
	return data
}
