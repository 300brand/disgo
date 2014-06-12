package disgo

import (
	"net"
	"strconv"
	"sync/atomic"
	"testing"
)

type MathService struct {
	Calls uint64
}
type N struct{ Value int }

func (m *MathService) Square(a, b *N) (err error) {
	atomic.AddUint64(&m.Calls, 1)
	b.Value = a.Value * a.Value
	return
}

func TestClientServer(t *testing.T) {
	s, err := NewServer([]string{etcdAddr}, localBroadcastAddr, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.RegisterName("Math", new(MathService)); err != nil {
		t.Fatal(err)
	}

	go func(addr string) {
		if err := s.Serve(addr); err != nil {
			t.Fatal(err)
		}
	}(localBroadcastAddr)

	c, err := NewClient([]string{etcdAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	result := new(N)
	if err := c.Call("Math.Square", N{3}, result); err != nil {
		t.Fatal(err)
	}

	if result.Value != 9 {
		t.Fatalf("Expected 9, got %d", result.Value)
	}
}

func TestClientMultiServer(t *testing.T) {
	host, portStr, _ := net.SplitHostPort(localBroadcastAddr)
	port, _ := strconv.Atoi(portStr)
	for i := 0; i < 3; i++ {
		listenAddr := net.JoinHostPort(host, strconv.Itoa(port+i))
		s, err := NewServer([]string{etcdAddr}, listenAddr, 2)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		svc := new(MathService)
		if err := s.RegisterName("Math", svc); err != nil {
			t.Fatal(err)
		}
		defer func(s *MathService, addr string) {
			t.Logf("MathService calls on %s: %d", addr, s.Calls)
		}(svc, listenAddr)

		go func(addr string) {
			if err := s.Serve(addr); err != nil {
				t.Fatal(err)
			}
		}(listenAddr)
	}

	c, err := NewClient([]string{etcdAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 3*100; i++ {
		result := new(N)
		if err := c.Call("Math.Square", N{3}, result); err != nil {
			t.Fatal(err)
		}

		if result.Value != 9 {
			t.Fatalf("Expected 9, got %d", result.Value)
		}
	}
}
