// Based on https://github.com/kuritka/go-fake-dns
// License: MIT

package dnsmock

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type DNSRecordError struct {
	Err error
}

func (d DNSRecordError) Error() string {
	return d.Err.Error()
}

// Server acts as DNS server but returns mock values.
type Server struct {
	server     *dns.Server
	records    map[uint16][]dns.RR
	updateLock *sync.Mutex
}

func New(port int) *Server {
	server := &Server{
		records:    make(map[uint16][]dns.RR),
		updateLock: &sync.Mutex{},
	}

	var handler dns.HandlerFunc = server.handleRequest

	server.server = &dns.Server{
		Addr:    "[::]:" + strconv.FormatInt(int64(port), 10),
		Net:     "udp",
		Handler: handler,
	}

	return server
}

func (s *Server) Start() error {
	startedCh := make(chan struct{})
	s.server.NotifyStartedFunc = func() {
		close(startedCh)
	}

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- s.server.ListenAndServe()
	}()

	// Wait for DNS server startup
	select {
	case <-startedCh:
	// ok
	case err := <-serverErrCh:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("DNS server start timeout")
	}

	return nil
}

func (s *Server) Addr() string {
	return s.server.PacketConn.LocalAddr().String()
}

func (s *Server) Shutdown() error {
	return s.server.Shutdown()
}

func (s *Server) AddTXTRecord(fqdn string, strings ...string) {
	t := &dns.TXT{
		Hdr: dns.RR_Header{Name: fqdn, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 0},
		Txt: strings,
	}
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	s.records[dns.TypeTXT] = append(s.records[dns.TypeTXT], t)
}

func (s *Server) AddNSRecord(fqdn, nsName string) {
	ns := &dns.NS{
		Hdr: dns.RR_Header{Name: fqdn, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: 0},
		Ns:  nsName,
	}
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	s.records[dns.TypeNS] = append(s.records[dns.TypeNS], ns)
}

func (s *Server) AddARecord(fqdn string, ip net.IP) error {
	ipv4 := ip.To4()
	if ipv4 == nil {
		return &DNSRecordError{Err: errors.New("Unable to create A record")}
	}

	rr := &dns.A{
		Hdr: dns.RR_Header{Name: fqdn, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 0},
		A:   ip.To4(),
	}
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	s.records[dns.TypeA] = append(s.records[dns.TypeA], rr)
	return nil
}

func (s *Server) AddAAAARecord(fqdn string, ip net.IP) error {
	ipv6 := ip.To16()
	if ipv6 == nil {
		return &DNSRecordError{Err: errors.New("Unable to create AAAA record")}
	}
	rr := &dns.AAAA{
		Hdr:  dns.RR_Header{Name: fqdn, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 0},
		AAAA: ip.To16(),
	}
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	s.records[dns.TypeAAAA] = append(s.records[dns.TypeAAAA], rr)
	return nil
}

func (s *Server) RemoveTXTRecords(fqdn string) {
	s.removeRecords(dns.TypeTXT, fqdn)
}

func (s *Server) RemoveNSRecords(fqdn string) {
	s.removeRecords(dns.TypeNS, fqdn)
}

func (s *Server) RemoveARecords(fqdn string) {
	s.removeRecords(dns.TypeA, fqdn)
}

func (s *Server) RemoveAAAARecords(fqdn string) {
	s.removeRecords(dns.TypeAAAA, fqdn)
}

func (s *Server) removeRecords(dnsType uint16, fqdn string) {
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	records := []dns.RR{}
	for _, rr := range s.records[dnsType] {
		if rr.Header().Name != fqdn {
			records = append(records, rr)
		}
	}
	s.records[dnsType] = records
}

func (s *Server) handleRequest(w dns.ResponseWriter, r *dns.Msg) {
	msg := new(dns.Msg)
	msg.SetReply(r)
	msg.Compress = false
	s.updateLock.Lock()
	defer s.updateLock.Unlock()
	if s.records[r.Question[0].Qtype] != nil {
		for _, rr := range s.records[r.Question[0].Qtype] {
			fqdn := strings.Split(rr.String(), "\t")[0]
			if fqdn == r.Question[0].Name {
				msg.Answer = append(msg.Answer, rr)
			}
		}
	}
	_ = w.WriteMsg(msg)
}
