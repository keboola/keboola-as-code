package chain

import "net/http"

type Chain struct {
	handler Handler
}

type Middleware func(next Handler) Handler

type Handler interface {
	ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error
}

type HandlerFunc func(w http.ResponseWriter, req *http.Request) error

func New(handler Handler) *Chain {
	return &Chain{handler: handler}
}

func (c *Chain) Prepend(fns ...Middleware) *Chain {
	for i := range fns {
		fn := fns[len(fns)-1-i]
		c.handler = fn(c.handler)
	}
	return c
}

func (c *Chain) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	return c.handler.ServeHTTPOrError(w, req)
}

func (fn HandlerFunc) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	return fn(w, req)
}
