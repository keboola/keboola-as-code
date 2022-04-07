package main

import (
	"net/http"

	goaHTTP "goa.design/goa/v3/http"
)

// decider decodes request.
func decoder(r *http.Request) goaHTTP.Decoder {
	return goaHTTP.RequestDecoder(r)
}
