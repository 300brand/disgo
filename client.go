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

var retryIncrement = 100 * time.Millisecond

func NewClient(machineAddrs []string) (c *Client, err error) {
	c = new(Client)
	c.conn = newEtcdConn(machineAddrs, "")
	return
}

func (c *Client) Call(f string, args, reply interface{}) (err error) {
	start := time.Now()
	serviceName := f[:strings.IndexByte(f, '.')]

	var addr string
	for i := 0; i < 3; i++ {
		if addr, err = c.conn.getAddr(serviceName); err == nil {
			break
		}
		retryDuration := time.Duration(i+1) * retryIncrement
		logger.Debug.Printf("No address for %s; waiting %s to retry", serviceName, retryDuration)
		<-time.After(retryDuration)
	}
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
