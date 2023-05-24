package middleware

import (
	"net/http"
)

const (
	maskedValue   = "****"
	maskedURLPart = "...."
)

type Middleware func(http.Handler) http.Handler

type ctxKey string

func Wrap(handler http.Handler, all ...Middleware) http.Handler {
	return Merge(all...)(handler)
}

func Merge(all ...Middleware) Middleware {
	return func(handler http.Handler) http.Handler {
		for i := len(all) - 1; i >= 0; i-- {
			handler = all[i](handler)
		}
		return handler
	}
}
