package httpserver

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"

type Config struct {
	ListenAddress     string
	ErrorNamePrefix   string
	ExceptionIDPrefix string
	MiddlewareOptions []middleware.Option
	// Mount endpoints to the Muxer
	Mount func(c Components)
}
