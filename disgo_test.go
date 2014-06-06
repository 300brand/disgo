package disgo

import (
	"fmt"
	"testing"
)

const nsqdAddr = "127.0.0.1:4150"
const nsqlookupdAddr = "127.0.0.1:4160"

type SquareSvc struct{}

type Num struct{ N int }

func (ss *SquareSvc) Square(a, b *Num) (err error) {
	if a.N == 1 {
		return fmt.Errorf("Can't square %d", a.N)
	}
	b.N = a.N * a.N
	return
}

func TestClient(t *testing.T) {
	if true {
		return
	}

	c, err := NewClient(nsqdAddr)
	if err != nil {
		t.Fatalf("Error new client: %s", err)
	}
	defer c.Close()

	args := struct{ A int }{2}
	reply := struct{ B int }{}
	if err := c.Call("square", args, &reply); err != nil {
		t.Fatal(err)
	}
}

func TestServer(t *testing.T) {
	if false {
		return
	}

	s, err := NewServer(nsqdAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.RegisterName("Testing.Square", new(SquareSvc))

	go func() {
		if err := s.Serve(""); err != nil {
			t.Fatal(err)
		}
	}()

	c, err := NewClient(nsqdAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	reply := new(Num)
	if err := c.Call("Testing.Square", Num{2}, reply); err != nil {
		t.Fatal(err)
	}
}
