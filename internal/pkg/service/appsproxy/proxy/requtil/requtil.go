package requtil

import (
	"net"
	"net/http"
)

func Host(req *http.Request) string {
	hostPort := req.URL.Host
	host, _, _ := net.SplitHostPort(hostPort)
	if host == "" {
		host = hostPort
	}
	return host
}
