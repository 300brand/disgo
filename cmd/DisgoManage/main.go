package main

import (
	"flag"
	"fmt"
	"github.com/miekg/dns"
	"github.com/nu7hatch/gouuid"
	"net"
	"os"
	"time"
)

var (
	Domain = flag.String("domain", "coverage.net", "Domain")
)

func main() {
	flag.Parse()

	switch cmd := flag.Arg(0); cmd {
	case "add":
		domain := dns.Fqdn(*Domain)
		serviceName, addr := flag.Arg(1), flag.Arg(2)
		ip, port, _ := net.SplitHostPort(addr)

		dnsName := fmt.Sprintf("%s.%s.%s.%s", port, ip, serviceName, domain)
		parent := fmt.Sprintf("%s.%s", serviceName, domain)

		u, err := uuid.NewV5(uuid.NamespaceDNS, []byte(dnsName))
		if err != nil {
			fmt.Printf("UUID Error: %s\n", err)
			os.Exit(1)
		}
		fmt.Println("UUID:", u)
		service := fmt.Sprintf("%s.%s", u, domain)

		fmt.Println("Parent:", parent)
		fmt.Println("Service:", service)
		fmt.Println("IP:", net.ParseIP(ip))

		// Question is Zone
		// Answer is Prerequisite
		// Authority is Update
		// only the Additional is not renamed.
		m := new(dns.Msg)
		m.SetUpdate(domain)
		m.Insert([]dns.RR{
			&dns.A{
				A: net.ParseIP("0.0.0.0"),
				Hdr: dns.RR_Header{
					Name:   parent,
					Ttl:    60,
					Rrtype: dns.TypeA,
				},
			},
			&dns.A{
				A: net.ParseIP(ip),
				Hdr: dns.RR_Header{
					Name:   dnsName,
					Ttl:    60,
					Rrtype: dns.TypeA,
				},
			},
			&dns.SRV{
				Hdr: dns.RR_Header{
					Name:   parent,
					Ttl:    60,
					Rrtype: dns.TypeSRV,
				},
				Port:     2000,
				Priority: 0,
				Weight:   0,
				Target:   dnsName,
			},
			&dns.TXT{
				Hdr: dns.RR_Header{
					Name:   dnsName,
					Ttl:    60,
					Rrtype: dns.TypeTXT,
				},
				Txt: []string{
					fmt.Sprintf("%d", time.Now().Add(time.Hour).UnixNano()),
					"FuncA",
					"FuncB",
					"FuncC",
				},
			},
		})

		fmt.Println(m)

		c := new(dns.Client)
		//c.TsigSecret = map[string]string{"coverage.net.": "so6ZGir4GPAqINNh9U5c3A=="}
		r, rtt, err := c.Exchange(m, "10.0.0.10:53")
		fmt.Printf("R: %+v\n\nRTT: %+v\n\nErr: %s\n", r, rtt, err)
	default:
		fmt.Printf("Don't know what to do with command: %s\n", cmd)
		os.Exit(1)
	}
}
