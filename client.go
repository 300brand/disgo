package disgo

import (
	"encoding/json"
	"github.com/jbaikge/logger"
	"github.com/mikespook/gearman-go/client"
	"time"
)

type Client struct {
	addrs  []string
	client *client.Client
}

func NewClient(addrs ...string) *Client {
	c, err := client.New(addrs[0])
	if err != nil {
		logger.Error.Printf("NewClient err: %s", err)
	}
	return &Client{
		addrs:  addrs,
		client: c,
	}
}

func (c *Client) Call(f string, in, out interface{}) (err error) {
	data, err := json.Marshal(in)
	if err != nil {
		return
	}

	ch := make(chan *client.Job, 1)
	handle := c.client.Do(f, data, client.JOB_NORMAL, func(j *client.Job) { ch <- j })
	logger.Info.Printf("%s handle: %s", f, handle)

	for {
		select {
		case job := <-ch:
			logger.Debug.Printf("Got job: %v", job.UniqueId)
			return json.Unmarshal(job.Data, out)
		case <-time.After(time.Second):
			logger.Debug.Printf("Checking status of %s", handle)
			status, err := c.client.Status(handle, time.Second)
			if err != nil {
				return err
			}
			logger.Info.Printf("%s running: %v", handle, status.Running)
		}
	}
	return
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) handler(job *client.Job) {
	logger.Info.Printf("Client handler! %+v", job)
}
