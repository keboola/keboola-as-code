package httpencode

import (
	"net/http"

	goaHTTP "goa.design/goa/v3/http"
)

// decider decodes request.
func NewDecoder() func(r *http.Request) goaHTTP.Decoder {
	return func(r *http.Request) goaHTTP.Decoder {
		return goaHTTP.RequestDecoder(r)
	}
}
