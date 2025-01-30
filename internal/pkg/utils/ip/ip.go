package ip

import (
	"net"
	"net/http"
	"strings"
)

const (
	XRealIPHeader       = "X-REAL-IP"
	XForwardedForHeader = "X-FORWARDED-FOR"
)

func From(r *http.Request) net.IP {
	if r == nil {
		return nil
	}

	// Get IP from the X-REAL-IP header
	ip := r.Header.Get(XRealIPHeader)
	if netIP := net.ParseIP(ip); netIP != nil {
		return netIP
	}

	// Get IP from X-FORWARDED-FOR header
	ips := r.Header.Get(XForwardedForHeader)
	splitIps := strings.Split(ips, ",")
	for _, ipport := range splitIps {
		ip, _, err := net.SplitHostPort(ipport)
		if err != nil {
			ip = ipport
		}
		if netIP := net.ParseIP(ip); netIP != nil {
			return netIP
		}
	}

	// Get IP from RemoteAddr
	ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	if netIP := net.ParseIP(ip); netIP != nil {
		return netIP
	}

	return nil
}
