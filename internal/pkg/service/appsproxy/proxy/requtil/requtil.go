package requtil

import (
	"net"
	"net/http"
)

func Host(req *http.Request) string {
	hostPort := HostPort(req)
	host, _, _ := net.SplitHostPort(hostPort)
	if host == "" {
		host = hostPort
	}
	return host
}

func HostPort(req *http.Request) string {
	host := req.Header.Get("X-Forwarded-For")
	if host == "" {
		host = req.Host
	}
	return host
}
