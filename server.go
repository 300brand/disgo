package disgo

import (
	"encoding/gob"
	"fmt"
	"github.com/300brand/logger"
	"net"
	"net/rpc"
)

type Server struct {
	conn     *etcdConn
	names    []string
	stops    map[string]chan bool
	stopChan chan bool
	gob      *rpc.Server
}

func init() {
	// Because sometimes you need a map of unknown things?
	gob.Register(make(map[string]interface{}))
	gob.Register([]interface{}(nil))
}

func NewServer(machineAddrs []string, broadcastAddr string, ttl uint64) (s *Server, err error) {
	s = &Server{
		gob:      rpc.NewServer(),
		names:    make([]string, 0, 64),
		stops:    make(map[string]chan bool, 64),
		stopChan: make(chan bool),
		conn:     newEtcdConn(machineAddrs, broadcastAddr),
	}
	s.conn.ttl = ttl
	return
}

func (s *Server) RegisterName(name string, rcvr interface{}) (err error) {
	if err = s.gob.RegisterName(name, rcvr); err != nil {
		return
	}
	s.names = append(s.names, name)
	return
}

func (s *Server) Serve(listenAddr string) (err error) {
	if len(s.names) == 0 {
		return fmt.Errorf("No services registered, nothing to serve.")
	}

	// Listen for requests
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return
	}
	go s.gob.Accept(listener)

	for _, name := range s.names {
		s.stops[name] = make(chan bool)
		go s.conn.announce(name, s.stops[name])
	}
	logger.Debug.Printf("Ready to accept connections")
	<-s.stopChan
	listener.Close()
	return
}

func (s *Server) Close() error {
	for name, ch := range s.stops {
		logger.Debug.Printf("Stopping %s", name)
		ch <- true
	}
	s.stopChan <- true
	return nil
}
