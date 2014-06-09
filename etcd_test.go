package disgo

import (
	"testing"
	"time"
)

const (
	etcdAddr           = "http://127.0.0.1:4002"
	localBroadcastAddr = "127.0.0.1:4003"
)

func TestRegister(t *testing.T) {
	e := newEtcdConn([]string{etcdAddr}, localBroadcastAddr)
	if err := e.register("TestService"); err != nil {
		t.Fatal(err)
	}
}

func TestGetAddr(t *testing.T) {
	e := newEtcdConn([]string{etcdAddr}, localBroadcastAddr)
	service := "TestService"
	if err := e.register(service); err != nil {
		t.Fatal(err)
	}
	addr, err := e.getAddr(service)
	if err != nil {
		t.Fatal(err)
	}
	if addr != localBroadcastAddr {
		t.Fatalf("Returned addr does not match: %s != %s", addr, localBroadcastAddr)
	}
}

func TestGetAddrNoneRegistered(t *testing.T) {
	e := newEtcdConn([]string{etcdAddr}, localBroadcastAddr)
	addr, err := e.getAddr("TestFakeService")
	if err == nil {
		t.Fatal("Expected an error")
	}
	if addr != "" {
		t.Fatalf("Returned an addr: %s", addr)
	}
}

func TestAnnounce(t *testing.T) {
	e := newEtcdConn([]string{etcdAddr}, localBroadcastAddr)
	service := "TestService"
	ch := make(chan bool)
	go e.announce(service, ch)

	for i := 0; i < 3; i++ {
		addr, err := e.getAddr(service)
		if err != nil {
			t.Fatal(err)
		}
		if addr != localBroadcastAddr {
			t.Fatalf("Returned addr does not match: %s != %s", addr, localBroadcastAddr)
		}

		if i == 2 {
			// trigger a stop
			ch <- true
		}
		<-time.After(time.Duration(e.ttl) * time.Second)
	}

	<-time.After(time.Second)

	addr, err := e.getAddr(service)
	if err == nil {
		t.Fatal("Expected an error")
	}
	if addr != "" {
		t.Fatalf("Returned an addr: %s", addr)
	}
}
