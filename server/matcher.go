package server

import (
	"github.com/jbaikge/disgo/logger"
	"time"
)

type HostMatcher interface {
	Add(string)
	Remove(string)
	Match(string) (string, error)
}

type DefaultMatcher struct {
	Hosts map[string]time.Time
}

var _ HostMatcher = new(DefaultMatcher)

func NewDefaultMatcher() *DefaultMatcher {
	return &DefaultMatcher{
		Hosts: make(map[string]time.Time),
	}
}

func (m *DefaultMatcher) Add(s string) {
	logger.Trace.Printf("DefaultMatcher.Add %s", s)
}
func (m *DefaultMatcher) Remove(s string) {
	logger.Trace.Printf("DefaultMatcher.Remove %s", s)

}
func (m *DefaultMatcher) Match(s string) (match string, err error) {
	logger.Trace.Printf("DefaultMatcher.Match %s", s)
	return
}
