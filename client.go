package disgo

import (
	"encoding/json"
	"github.com/300brand/logger"
	"github.com/mikespook/gearman-go/client"
	"math/rand"
	"reflect"
	"time"
)

type Client struct {
	addrs  []string
	client *client.Client
}

var (
	connections = make(chan bool, 128) // Max outbound connections to gearman
	rNullType   = reflect.TypeOf(Null)
)

func NewClient(addrs ...string) *Client {
	return &Client{
		addrs: addrs,
	}
}

func (c *Client) Call(f string, in, out interface{}) (err error) {
	data, err := json.Marshal(in)
	if err != nil {
		return
	}

	start := time.Now()

	connections <- true
	defer func() {
		<-connections
	}()

	cl, err := c.connect()
	if err != nil {
		return
	}
	defer cl.Close()

	ch := make(chan *client.Job, 1)
	logger.Trace.Printf("disgo.Client: [%s] SEND", f)

	var flag byte = client.JOB_NORMAL
	if reflect.TypeOf(out).ConvertibleTo(rNullType) {
		flag |= client.JOB_BG
	}

	handle := cl.Do(f, data, flag, func(j *client.Job) { ch <- j; close(ch) })

	logger.Debug.Printf("disgo.Client: [%s] HNDL %s", f, handle)

	defer func(f, h string) {
		logger.Trace.Printf("disgo.Client: [%s] DONE %s %s", f, h, time.Since(start))
	}(f, handle)

	for {
		select {
		case job := <-ch:
			logger.Trace.Printf("disgo.Client: [%s] RECV %s", f, handle)
			response := new(ResponseFromServer)
			if err = json.Unmarshal(job.Data, response); err != nil {
				logger.Error.Printf("disgo.Client: Unmarshal Error: %s", err)
				return
			}
			if err, ok := response.Error.(error); ok && err != nil {
				return err
			}
			return json.Unmarshal(*response.Result, out)
		case <-time.After(2 * time.Second):
			// status, err := c.client.Status(handle, 0)
			// if err != nil {
			// 	logger.Error.Printf("disgo.Client: [%s] (%s) Error checking status on %s: %s", f, time.Since(start), handle, err)
			// 	return err
			// }
			logger.Trace.Printf("disgo.Client [%s] RUNG %s", f, handle)
		}
	}
	return
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) connect() (cl *client.Client, err error) {
	for i := range rand.Perm(len(c.addrs)) {
		logger.Trace.Printf("disgo.Client: Connecting to %s", c.addrs[i])
		if cl, err = client.New(c.addrs[i]); err == nil {
			break
		}
	}
	return
}

func (c *Client) handler(job *client.Job) {
	logger.Debug.Printf("Client handler! %+v", job)
}
