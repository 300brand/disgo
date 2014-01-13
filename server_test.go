package disgo

import (
	"github.com/300brand/gearman-go/common"
	"github.com/300brand/gearman-go/worker"
	"testing"
)

type In struct{ A int }

type Out struct{ B int }

type Foo struct{}

func (f Foo) Bar(in *In, out *Out) error {
	out.B = in.A + 1
	return nil
}

func TestRegister(t *testing.T) {
	s := NewServer()
	if err := s.Register(&Foo{}); err != nil {
		t.Fatal(err)
	}
}

func TestHandleJob(t *testing.T) {
	s := NewServer()
	if err := s.Register(&Foo{}); err != nil {
		t.Fatal(err)
	}
	j := &worker.Job{
		Data:     []byte(`{"A": 1}`),
		Handle:   "H:foobar:100",
		UniqueId: "8e1dd1aa-2b72-11e3-b54e-00e0817970a4",
		Fn:       "Foo.Bar",
		DataType: common.NOOP,
	}
	data, err := s.handleJob(j)
	if err != nil {
		t.Error(err)
	}
	t.Logf("%s", data)
}

func TestHandleJobErrors(t *testing.T) {
	s := NewServer()
	if err := s.Register(&Foo{}); err != nil {
		t.Fatal(err)
	}
	j := &worker.Job{
		Data:     []byte(`{"A": 1}`),
		Handle:   "H:foobar:100",
		UniqueId: "8e1dd1aa-2b72-11e3-b54e-00e0817970a4",
		DataType: common.NOOP,
	}
	badFns := []string{"Foo", "FooBar", "Foo.Baz", "Foo.Bar.Baz"}
	for _, fn := range badFns {
		j.Fn = fn
		if _, err := s.handleJob(j); err == nil {
			t.Errorf("Expected error for Fn = %s", fn)
		}
	}
}
