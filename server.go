package disgo

import (
	"bytes"
	"fmt"
	"github.com/300brand/logger"
	"github.com/kr/beanstalk"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
	"time"
)

type Server struct {
	conn  *beanstalk.Conn
	names []string
	gob   *rpc.Server
	json  *rpc.Server
}

const longDur = 100 * 365 * 24 * time.Hour

func NewServer(addr string) (s *Server, err error) {
	s = &Server{
		gob:   rpc.NewServer(),
		json:  rpc.NewServer(),
		names: make([]string, 0, 64),
	}
	s.gob.HandleHTTP("/gob/rpc", "/gob/debug")
	s.json.HandleHTTP("/json/rpc", "/json/debug")
	s.conn, err = beanstalk.Dial("tcp", addr)
	return
}

func (s *Server) RegisterName(name string, rcvr interface{}) (err error) {
	if err = s.gob.RegisterName(name, rcvr); err != nil {
		return
	}
	if err = s.json.RegisterName(name, rcvr); err != nil {
		return
	}
	s.names = append(s.names, name)
	return
}

func (s *Server) Serve(listenAddr string) (err error) {
	if len(s.names) == 0 {
		return fmt.Errorf("No services registered, nothing to serve.")
	}

	httpAddr, gobAddr, jsonAddr := listeners(listenAddr)
	// Pre-cast addresses to []byte for transport
	httpBytes := []byte(httpAddr.Addr().String())
	gobBytes := []byte(gobAddr.Addr().String())
	jsonBytes := []byte(jsonAddr.Addr().String())

	// Listen for HTTP requests
	go http.Serve(httpAddr, nil)
	go s.gob.Accept(gobAddr)
	go s.acceptJSON(jsonAddr)

	requestTube := beanstalk.NewTubeSet(s.conn, s.names...)

	var rpcAddr []byte
	for {
		requestId, req, err := requestTube.Reserve(longDur)
		if err != nil {
			logger.Error.Printf("Error reading from requestTube: %s", err)
			continue
		}

		rpcType, serviceName := req[:4], req[4:]
		switch {
		case bytes.Equal(RPCGOB, rpcType):
			rpcAddr = gobBytes
		case bytes.Equal(RPCJSON, rpcType):
			rpcAddr = jsonBytes
		case bytes.Equal(RPCHTTP, rpcType):
			rpcAddr = httpBytes
		default:
			rpcAddr = []byte(`invalid`)
		}

		defer func(start time.Time) {
			logger.Debug.Printf("disgo.Server:%d %s for %s@%s took %s", requestId, rpcType, serviceName, rpcAddr, time.Since(start))
		}(time.Now())

		// Generate name, hopefully matching the name of the tube the request
		// came in on.
		name := fmt.Sprintf("%s.%d", serviceName, requestId)
		logger.Trace.Printf("Sending %s along %s", rpcAddr, name)

		responseTube := beanstalk.Tube{Conn: s.conn, Name: name}

		if _, err := responseTube.Put(rpcAddr, 1, 0, time.Minute); err != nil {
			logger.Error.Printf("Error writing to responseTube: %s", err)
			continue
		}

		// Remove request
		requestTube.Conn.Delete(requestId)
	}

	return
}

func (s *Server) Close() error {
	return s.conn.Close()
}

func (s *Server) acceptJSON(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Error.Printf("Error accepting connection: %s", err)
			continue
		}
		go s.json.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}

func listeners(listenAddr string) (net.Listener, net.Listener, net.Listener) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		logger.Error.Fatal(err)
	}
	start, err := strconv.Atoi(port)
	if err != nil {
		logger.Error.Fatalf("Port is not numeric: %s", port)
	}

	listeners := make(chan net.Listener, 3)
	defer close(listeners)
	var c int
	for p := start; p < 65535 && c < 3; p++ {
		l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, p))
		if err != nil {
			continue
		}
		c++
		listeners <- l
	}
	if c != 3 {
		panic("Could not listen on 3 ports!")
	}
	return <-listeners, <-listeners, <-listeners
}
