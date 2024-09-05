package pprof

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

func NewHTTPServer(addr string) *http.Server {
	runtime.SetBlockProfileRate(20)     // 5%
	runtime.SetMutexProfileFraction(20) // 5%
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &http.Server{Addr: addr, ReadHeaderTimeout: 10 * time.Second, Handler: mux}
}
