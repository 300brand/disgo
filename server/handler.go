package server

import (
	"github.com/jbaikge/disgo/logger"
	"github.com/miekg/dns"
)

type Handler struct {
	Matcher HostMatcher
}

var _ dns.Handler = new(Handler)

func NewHandler() *Handler {
	return &Handler{
		Matcher: NewDefaultMatcher(),
	}
}

func (h *Handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	logger.Trace.Printf("[%d] RemoteAddr: %s", r.Id, w.RemoteAddr())
	logger.Trace.Printf("[%d] Msg: %+v", r.Id, *r)

	reply := new(dns.Msg)
	reply.SetReply(r)

	switch opcode := r.Opcode; opcode {
	case dns.OpcodeUpdate:
		logger.Debug.Print("Got an update")
		for _, ns := range r.Ns {
			h := ns.Header()
			logger.Debug.Printf("Header: %+v", h)
			logger.Debug.Printf("%+v", h.Name)
			logger.Debug.Printf("%+v", h.Ttl)
			logger.Debug.Printf("%+v", h.)
		}
	case dns.OpcodeQuery:
		logger.Debug.Print("Got a query")
	default:
		logger.Debug.Printf("Not sure what to do with [%d] %s", opcode, dns.OpcodeToString[opcode])
	}

	if err := w.WriteMsg(reply); err != nil {
		logger.Error.Print(err)
	}
}
