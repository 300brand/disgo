package main

import (
	"github.com/jbaikge/disgo/logger"
	"github.com/jbaikge/disgo/server"
	"github.com/miekg/dns"
)

func main() {
	logger.Trace.Print("Trace")
	logger.Debug.Print("Debug")
	logger.Info.Print("Info")
	logger.Warn.Print("Warn")
	logger.Error.Print("Error")

	logger.Trace.Print("Starting DisgoServer")

	s := dns.NewServeMux()
	s.Handle(".", server.NewHandler())

	logger.Error.Fatal(dns.ListenAndServe(":10100", "udp", s))
}
