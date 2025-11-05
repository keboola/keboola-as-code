package httpserver

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"

type Config struct {
	ListenAddress     string
	ErrorNamePrefix   string
	ExceptionIDPrefix string
	MiddlewareOptions []middleware.Option
	// Enable gzip compression for responses
	EnableGzip bool
	// Mount endpoints to the Muxer
	Mount func(c Components)
	// BeforeLoggerMiddlewares are middlewares that should run before the Logger middleware.
	// This allows service-specific middlewares (e.g., ProjectAttributes, Telemetry) to enrich
	// the context before logging, so the Logger can include those attributes in the log output.
	BeforeLoggerMiddlewares []middleware.Middleware
}
