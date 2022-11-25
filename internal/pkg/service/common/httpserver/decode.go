package httpserver

import (
	"net/http"

	goaHTTP "goa.design/goa/v3/http"
)

func NewDecoder() func(r *http.Request) goaHTTP.Decoder {
	return func(r *http.Request) goaHTTP.Decoder {
		return goaHTTP.RequestDecoder(r)
	}
}
