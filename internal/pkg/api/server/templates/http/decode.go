package http

import (
	"net/http"

	goaHTTP "goa.design/goa/v3/http"
)

// decider decodes request.
func newDecoder() func(r *http.Request) goaHTTP.Decoder {
	return func(r *http.Request) goaHTTP.Decoder {
		return goaHTTP.RequestDecoder(r)
	}
}
