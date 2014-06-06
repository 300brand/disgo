package disgo

import (
	"fmt"
	"github.com/300brand/logger"
	"github.com/bitly/go-nsq"
	"net/rpc"
	"os"
)

type Client struct {
	producer *nsq.Producer // nsq producer
	ident    string        // ident for this specific client - a separate channel will be used
}

func NewClient(addr string) (c *Client, err error) {
	config := nsq.NewConfig()
	config.Set("verbose", true)

	c = new(Client)
	c.producer = nsq.NewProducer(addr, config)

	hostname, err := os.Hostname()
	if err != nil {
		return
	}
	c.ident = fmt.Sprintf("%d-%d", hostname, os.Getpid())
	return
}

func (c *Client) Call(f string, args, reply interface{}) (err error) {
	buf := new(Buffer)
	client := rpc.NewClient(buf)
	defer func() {
		err := client.Close()
		if err != nil {
			logger.Error.Printf("Error closing client: %s", err)
		}
	}()

	done := make(chan *rpc.Call, 1)
	call := client.Go(f, args, reply, done)

	logger.Info.Printf("ServiceMethod: %s", call.ServiceMethod)
	// Hijack the contents of the buffer and send as a message
	logger.Info.Printf("Buffer Size: %d", buf.Len())

	err = c.producer.Publish(f, buf.Bytes())

	return
}

func (c *Client) Close() error {
	c.producer.Stop()
	return nil
}
