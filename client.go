package disgo

import (
	"github.com/300brand/logger"
	"net/rpc"
	"strings"
	"time"
)

type Client struct {
	conn *etcdConn
}

func NewClient(machineAddrs []string) (c *Client, err error) {
	c = new(Client)
	c.conn = newEtcdConn(machineAddrs, "")
	return
}

func (c *Client) Call(f string, args, reply interface{}) (err error) {
	start := time.Now()
	logger.Trace.Printf("disgo.Client: Calling %s", f)

	serviceName := f[:strings.IndexByte(f, '.')]

	addr, err := c.conn.getAddr(serviceName)
	if err != nil {
		return
	}

	// Connect to the RPC handler and perform the action
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()

	defer func(start time.Time) {
		logger.Debug.Printf("disgo.Client:%s @ %s took %s", f, addr, time.Since(start))
	}(start)

	return client.Call(f, args, reply)
}

func (c *Client) Close() error {
	return nil
}
