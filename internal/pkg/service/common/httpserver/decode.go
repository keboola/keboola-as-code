package httpserver

import (
	"net/http"

	goaHTTP "goa.design/goa/v3/http"
)

type Decoder func(*http.Request) goaHTTP.Decoder

func NewDecoder() Decoder {
	return goaHTTP.RequestDecoder
}
