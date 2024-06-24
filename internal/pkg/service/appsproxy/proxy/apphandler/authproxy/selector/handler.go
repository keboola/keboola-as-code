package selector

import (
	"net/http"
	"time"
)

type Handler interface {
	Name() string
	ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error
	CookieExpiration() time.Duration
	SignInPath() string
}
