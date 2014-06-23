package disgo

import (
	"fmt"
	"github.com/300brand/logger"
	"github.com/coreos/go-etcd/etcd"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

type etcdConn struct {
	broadcast  string
	cachedName string
	client     *etcd.Client
	ttl        uint64
}

const (
	gobDir   = "/disgo/gobrpc/"
	nodesDir = "/disgo/nodes/"
)

func newEtcdConn(machines []string, broadcastAddr string) (e *etcdConn) {
	e = &etcdConn{
		broadcast: broadcastAddr,
		ttl:       2,
	}
	e.client = etcd.NewClient(machines)
	rand.Seed(time.Now().UnixNano())
	return
}

func (e *etcdConn) announce(service string, stopChan chan bool) {
	if err := e.register(service); err != nil {
		logger.Warn.Printf("Error registering %s: %s", service, err)
	}
	for {
		select {
		case <-stopChan:
			return
		case <-time.After(time.Duration(e.ttl) * time.Second / 2):
			if err := e.register(service); err != nil {
				logger.Warn.Printf("Error registering %s: %s", service, err)
			}
		}
	}
}

func (e *etcdConn) getAddr(service string) (addr string, err error) {
	response, err := e.client.Get(filepath.Join(gobDir, service), true, false)
	if err != nil {
		return
	}

	nodes := response.Node.Nodes
	if len(nodes) == 0 {
		err = fmt.Errorf("No machines registered for %s", service)
		return
	}
	node := nodes[rand.Intn(len(nodes))]
	addr = node.Value
	return
}

// Registers the service with the appropriate TTL. To automatically re-
// register the service before the TTL, use/ etcdConn.announce(service).
func (e *etcdConn) register(service string) (err error) {
	keys := []string{
		filepath.Join(gobDir, service, e.machineName()),
		filepath.Join(nodesDir, e.machineName()),
	}
	for _, key := range keys {
		if _, err := e.client.Update(key, e.broadcast, e.ttl); err != nil {
			logger.Warn.Printf("Error updating %s: %s", service, err)
		} else {
			continue
		}
		if _, err := e.client.Set(key, e.broadcast, e.ttl); err != nil {
			logger.Error.Printf("Error registering %s: %s", service, err)
		}
	}
	return
}

func (e *etcdConn) machineName() string {
	if e.cachedName != "" {
		return e.cachedName
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	e.cachedName = fmt.Sprintf("%s-%d-%d-%04X",
		hostname,
		os.Getpid(),
		time.Now().Unix(),
		rand.Intn(0xFFFF),
	)
	return e.cachedName
}
