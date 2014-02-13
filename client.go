package disgo

import (
	"fmt"
	"github.com/300brand/logger"
	"github.com/kr/beanstalk"
	"net/rpc"
	"strings"
	"time"
)

type Client struct {
	conn *beanstalk.Conn
}

func NewClient(addr string) (c *Client, err error) {
	c = new(Client)
	c.conn, err = beanstalk.Dial("tcp", addr)
	return
}

func (c *Client) Call(f string, args, reply interface{}) (err error) {
	start := time.Now()

	serviceName := f[:strings.IndexByte(f, '.')]

	// Request access to the GOB RPC handler
	requestTube := beanstalk.Tube{Conn: c.conn, Name: serviceName}
	requestId, err := requestTube.Put(append(RPCGOB, []byte(serviceName)...), 1, 0, 15*time.Minute)
	if err != nil {
		return
	}

	// Make a new tube [serviceName.requestId] to send the RPC address from
	// server -> client
	name := fmt.Sprintf("%s.%d", serviceName, requestId)
	responseTube := beanstalk.NewTubeSet(c.conn, name)
	responseId, addr, err := responseTube.Reserve(time.Minute)
	if err != nil {
		return
	}
	responseTube.Conn.Delete(responseId)

	// Connect to the RPC handler and perform the action
	client, err := rpc.Dial("tcp", string(addr))
	if err != nil {
		return
	}
	defer client.Close()

	defer func(start time.Time) {
		logger.Debug.Printf("disgo.Client:%d %s@%s took %s", requestId, f, addr, time.Since(start))
	}(start)

	return client.Call(f, args, reply)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
