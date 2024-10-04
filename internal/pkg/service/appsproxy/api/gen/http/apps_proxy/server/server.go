// Code generated by goa v3.16.2, DO NOT EDIT.
//
// apps-proxy HTTP server
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/appsproxy --output
// ./internal/pkg/service/appsproxy/api

package server

import (
	"context"
	"net/http"

	appsproxy "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/apps_proxy"
	goahttp "goa.design/goa/v3/http"
	goa "goa.design/goa/v3/pkg"
	"goa.design/plugins/v3/cors"
)

// Server lists the apps-proxy service endpoint HTTP handlers.
type Server struct {
	Mounts          []*MountPoint
	APIRootIndex    http.Handler
	APIVersionIndex http.Handler
	HealthCheck     http.Handler
	Validate        http.Handler
	CORS            http.Handler
	OpenapiJSON     http.Handler
	OpenapiYaml     http.Handler
	Openapi3JSON    http.Handler
	Openapi3Yaml    http.Handler
	SwaggerUI       http.Handler
}

// MountPoint holds information about the mounted endpoints.
type MountPoint struct {
	// Method is the name of the service method served by the mounted HTTP handler.
	Method string
	// Verb is the HTTP method used to match requests to the mounted handler.
	Verb string
	// Pattern is the HTTP request path pattern used to match requests to the
	// mounted handler.
	Pattern string
}

// New instantiates HTTP handlers for all the apps-proxy service endpoints
// using the provided encoder and decoder. The handlers are mounted on the
// given mux using the HTTP verb and path defined in the design. errhandler is
// called whenever a response fails to be encoded. formatter is used to format
// errors returned by the service methods prior to encoding. Both errhandler
// and formatter are optional and can be nil.
func New(
	e *appsproxy.Endpoints,
	mux goahttp.Muxer,
	decoder func(*http.Request) goahttp.Decoder,
	encoder func(context.Context, http.ResponseWriter) goahttp.Encoder,
	errhandler func(context.Context, http.ResponseWriter, error),
	formatter func(ctx context.Context, err error) goahttp.Statuser,
	fileSystemOpenapiJSON http.FileSystem,
	fileSystemOpenapiYaml http.FileSystem,
	fileSystemOpenapi3JSON http.FileSystem,
	fileSystemOpenapi3Yaml http.FileSystem,
	fileSystemSwaggerUI http.FileSystem,
) *Server {
	if fileSystemOpenapiJSON == nil {
		fileSystemOpenapiJSON = http.Dir(".")
	}
	if fileSystemOpenapiYaml == nil {
		fileSystemOpenapiYaml = http.Dir(".")
	}
	if fileSystemOpenapi3JSON == nil {
		fileSystemOpenapi3JSON = http.Dir(".")
	}
	if fileSystemOpenapi3Yaml == nil {
		fileSystemOpenapi3Yaml = http.Dir(".")
	}
	if fileSystemSwaggerUI == nil {
		fileSystemSwaggerUI = http.Dir(".")
	}
	return &Server{
		Mounts: []*MountPoint{
			{"APIRootIndex", "GET", "/_proxy/api/"},
			{"APIVersionIndex", "GET", "/_proxy/api/v1"},
			{"HealthCheck", "GET", "/_proxy/api/v1/health-check"},
			{"Validate", "GET", "/_proxy/api/v1/validate"},
			{"CORS", "OPTIONS", "/_proxy/api/"},
			{"CORS", "OPTIONS", "/_proxy/api/v1"},
			{"CORS", "OPTIONS", "/_proxy/api/v1/health-check"},
			{"CORS", "OPTIONS", "/_proxy/api/v1/validate"},
			{"CORS", "OPTIONS", "/_proxy/api/v1/documentation/openapi.json"},
			{"CORS", "OPTIONS", "/_proxy/api/v1/documentation/openapi.yaml"},
			{"CORS", "OPTIONS", "/_proxy/api/v1/documentation/openapi3.json"},
			{"CORS", "OPTIONS", "/_proxy/api/v1/documentation/openapi3.yaml"},
			{"CORS", "OPTIONS", "/_proxy/api/v1/documentation/{*path}"},
			{"openapi.json", "GET", "/_proxy/api/v1/documentation/openapi.json"},
			{"openapi.yaml", "GET", "/_proxy/api/v1/documentation/openapi.yaml"},
			{"openapi3.json", "GET", "/_proxy/api/v1/documentation/openapi3.json"},
			{"openapi3.yaml", "GET", "/_proxy/api/v1/documentation/openapi3.yaml"},
			{"swagger-ui", "GET", "/_proxy/api/v1/documentation"},
		},
		APIRootIndex:    NewAPIRootIndexHandler(e.APIRootIndex, mux, decoder, encoder, errhandler, formatter),
		APIVersionIndex: NewAPIVersionIndexHandler(e.APIVersionIndex, mux, decoder, encoder, errhandler, formatter),
		HealthCheck:     NewHealthCheckHandler(e.HealthCheck, mux, decoder, encoder, errhandler, formatter),
		Validate:        NewValidateHandler(e.Validate, mux, decoder, encoder, errhandler, formatter),
		CORS:            NewCORSHandler(),
		OpenapiJSON:     http.FileServer(fileSystemOpenapiJSON),
		OpenapiYaml:     http.FileServer(fileSystemOpenapiYaml),
		Openapi3JSON:    http.FileServer(fileSystemOpenapi3JSON),
		Openapi3Yaml:    http.FileServer(fileSystemOpenapi3Yaml),
		SwaggerUI:       http.FileServer(fileSystemSwaggerUI),
	}
}

// Service returns the name of the service served.
func (s *Server) Service() string { return "apps-proxy" }

// Use wraps the server handlers with the given middleware.
func (s *Server) Use(m func(http.Handler) http.Handler) {
	s.APIRootIndex = m(s.APIRootIndex)
	s.APIVersionIndex = m(s.APIVersionIndex)
	s.HealthCheck = m(s.HealthCheck)
	s.Validate = m(s.Validate)
	s.CORS = m(s.CORS)
}

// MethodNames returns the methods served.
func (s *Server) MethodNames() []string { return appsproxy.MethodNames[:] }

// Mount configures the mux to serve the apps-proxy endpoints.
func Mount(mux goahttp.Muxer, h *Server) {
	MountAPIRootIndexHandler(mux, h.APIRootIndex)
	MountAPIVersionIndexHandler(mux, h.APIVersionIndex)
	MountHealthCheckHandler(mux, h.HealthCheck)
	MountValidateHandler(mux, h.Validate)
	MountCORSHandler(mux, h.CORS)
	MountOpenapiJSON(mux, goahttp.Replace("", "/openapi.json", h.OpenapiJSON))
	MountOpenapiYaml(mux, goahttp.Replace("", "/openapi.yaml", h.OpenapiYaml))
	MountOpenapi3JSON(mux, goahttp.Replace("", "/openapi3.json", h.Openapi3JSON))
	MountOpenapi3Yaml(mux, goahttp.Replace("", "/openapi3.yaml", h.Openapi3Yaml))
	MountSwaggerUI(mux, goahttp.Replace("/_proxy/api/v1/documentation", "/swagger-ui", h.SwaggerUI))
}

// Mount configures the mux to serve the apps-proxy endpoints.
func (s *Server) Mount(mux goahttp.Muxer) {
	Mount(mux, s)
}

// MountAPIRootIndexHandler configures the mux to serve the "apps-proxy"
// service "ApiRootIndex" endpoint.
func MountAPIRootIndexHandler(mux goahttp.Muxer, h http.Handler) {
	f, ok := HandleAppsProxyOrigin(h).(http.HandlerFunc)
	if !ok {
		f = func(w http.ResponseWriter, r *http.Request) {
			h.ServeHTTP(w, r)
		}
	}
	mux.Handle("GET", "/_proxy/api/", f)
}

// NewAPIRootIndexHandler creates a HTTP handler which loads the HTTP request
// and calls the "apps-proxy" service "ApiRootIndex" endpoint.
func NewAPIRootIndexHandler(
	endpoint goa.Endpoint,
	mux goahttp.Muxer,
	decoder func(*http.Request) goahttp.Decoder,
	encoder func(context.Context, http.ResponseWriter) goahttp.Encoder,
	errhandler func(context.Context, http.ResponseWriter, error),
	formatter func(ctx context.Context, err error) goahttp.Statuser,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), goahttp.AcceptTypeKey, r.Header.Get("Accept"))
		ctx = context.WithValue(ctx, goa.MethodKey, "ApiRootIndex")
		ctx = context.WithValue(ctx, goa.ServiceKey, "apps-proxy")
		http.Redirect(w, r, "/_proxy/api/v1/", http.StatusMovedPermanently)
	})
}

// MountAPIVersionIndexHandler configures the mux to serve the "apps-proxy"
// service "ApiVersionIndex" endpoint.
func MountAPIVersionIndexHandler(mux goahttp.Muxer, h http.Handler) {
	f, ok := HandleAppsProxyOrigin(h).(http.HandlerFunc)
	if !ok {
		f = func(w http.ResponseWriter, r *http.Request) {
			h.ServeHTTP(w, r)
		}
	}
	mux.Handle("GET", "/_proxy/api/v1", f)
}

// NewAPIVersionIndexHandler creates a HTTP handler which loads the HTTP
// request and calls the "apps-proxy" service "ApiVersionIndex" endpoint.
func NewAPIVersionIndexHandler(
	endpoint goa.Endpoint,
	mux goahttp.Muxer,
	decoder func(*http.Request) goahttp.Decoder,
	encoder func(context.Context, http.ResponseWriter) goahttp.Encoder,
	errhandler func(context.Context, http.ResponseWriter, error),
	formatter func(ctx context.Context, err error) goahttp.Statuser,
) http.Handler {
	var (
		encodeResponse = EncodeAPIVersionIndexResponse(encoder)
		encodeError    = goahttp.ErrorEncoder(encoder, formatter)
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), goahttp.AcceptTypeKey, r.Header.Get("Accept"))
		ctx = context.WithValue(ctx, goa.MethodKey, "ApiVersionIndex")
		ctx = context.WithValue(ctx, goa.ServiceKey, "apps-proxy")
		var err error
		res, err := endpoint(ctx, nil)
		if err != nil {
			if err := encodeError(ctx, w, err); err != nil {
				errhandler(ctx, w, err)
			}
			return
		}
		if err := encodeResponse(ctx, w, res); err != nil {
			errhandler(ctx, w, err)
		}
	})
}

// MountHealthCheckHandler configures the mux to serve the "apps-proxy" service
// "HealthCheck" endpoint.
func MountHealthCheckHandler(mux goahttp.Muxer, h http.Handler) {
	f, ok := HandleAppsProxyOrigin(h).(http.HandlerFunc)
	if !ok {
		f = func(w http.ResponseWriter, r *http.Request) {
			h.ServeHTTP(w, r)
		}
	}
	mux.Handle("GET", "/_proxy/api/v1/health-check", f)
}

// NewHealthCheckHandler creates a HTTP handler which loads the HTTP request
// and calls the "apps-proxy" service "HealthCheck" endpoint.
func NewHealthCheckHandler(
	endpoint goa.Endpoint,
	mux goahttp.Muxer,
	decoder func(*http.Request) goahttp.Decoder,
	encoder func(context.Context, http.ResponseWriter) goahttp.Encoder,
	errhandler func(context.Context, http.ResponseWriter, error),
	formatter func(ctx context.Context, err error) goahttp.Statuser,
) http.Handler {
	var (
		encodeResponse = EncodeHealthCheckResponse(encoder)
		encodeError    = goahttp.ErrorEncoder(encoder, formatter)
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), goahttp.AcceptTypeKey, r.Header.Get("Accept"))
		ctx = context.WithValue(ctx, goa.MethodKey, "HealthCheck")
		ctx = context.WithValue(ctx, goa.ServiceKey, "apps-proxy")
		var err error
		res, err := endpoint(ctx, nil)
		if err != nil {
			if err := encodeError(ctx, w, err); err != nil {
				errhandler(ctx, w, err)
			}
			return
		}
		if err := encodeResponse(ctx, w, res); err != nil {
			errhandler(ctx, w, err)
		}
	})
}

// MountValidateHandler configures the mux to serve the "apps-proxy" service
// "Validate" endpoint.
func MountValidateHandler(mux goahttp.Muxer, h http.Handler) {
	f, ok := HandleAppsProxyOrigin(h).(http.HandlerFunc)
	if !ok {
		f = func(w http.ResponseWriter, r *http.Request) {
			h.ServeHTTP(w, r)
		}
	}
	mux.Handle("GET", "/_proxy/api/v1/validate", f)
}

// NewValidateHandler creates a HTTP handler which loads the HTTP request and
// calls the "apps-proxy" service "Validate" endpoint.
func NewValidateHandler(
	endpoint goa.Endpoint,
	mux goahttp.Muxer,
	decoder func(*http.Request) goahttp.Decoder,
	encoder func(context.Context, http.ResponseWriter) goahttp.Encoder,
	errhandler func(context.Context, http.ResponseWriter, error),
	formatter func(ctx context.Context, err error) goahttp.Statuser,
) http.Handler {
	var (
		decodeRequest  = DecodeValidateRequest(mux, decoder)
		encodeResponse = EncodeValidateResponse(encoder)
		encodeError    = goahttp.ErrorEncoder(encoder, formatter)
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), goahttp.AcceptTypeKey, r.Header.Get("Accept"))
		ctx = context.WithValue(ctx, goa.MethodKey, "Validate")
		ctx = context.WithValue(ctx, goa.ServiceKey, "apps-proxy")
		payload, err := decodeRequest(r)
		if err != nil {
			if err := encodeError(ctx, w, err); err != nil {
				errhandler(ctx, w, err)
			}
			return
		}
		res, err := endpoint(ctx, payload)
		if err != nil {
			if err := encodeError(ctx, w, err); err != nil {
				errhandler(ctx, w, err)
			}
			return
		}
		if err := encodeResponse(ctx, w, res); err != nil {
			errhandler(ctx, w, err)
		}
	})
}

// MountOpenapiJSON configures the mux to serve GET request made to
// "/_proxy/api/v1/documentation/openapi.json".
func MountOpenapiJSON(mux goahttp.Muxer, h http.Handler) {
	mux.Handle("GET", "/_proxy/api/v1/documentation/openapi.json", HandleAppsProxyOrigin(h).ServeHTTP)
}

// MountOpenapiYaml configures the mux to serve GET request made to
// "/_proxy/api/v1/documentation/openapi.yaml".
func MountOpenapiYaml(mux goahttp.Muxer, h http.Handler) {
	mux.Handle("GET", "/_proxy/api/v1/documentation/openapi.yaml", HandleAppsProxyOrigin(h).ServeHTTP)
}

// MountOpenapi3JSON configures the mux to serve GET request made to
// "/_proxy/api/v1/documentation/openapi3.json".
func MountOpenapi3JSON(mux goahttp.Muxer, h http.Handler) {
	mux.Handle("GET", "/_proxy/api/v1/documentation/openapi3.json", HandleAppsProxyOrigin(h).ServeHTTP)
}

// MountOpenapi3Yaml configures the mux to serve GET request made to
// "/_proxy/api/v1/documentation/openapi3.yaml".
func MountOpenapi3Yaml(mux goahttp.Muxer, h http.Handler) {
	mux.Handle("GET", "/_proxy/api/v1/documentation/openapi3.yaml", HandleAppsProxyOrigin(h).ServeHTTP)
}

// MountSwaggerUI configures the mux to serve GET request made to
// "/_proxy/api/v1/documentation".
func MountSwaggerUI(mux goahttp.Muxer, h http.Handler) {
	mux.Handle("GET", "/_proxy/api/v1/documentation/", HandleAppsProxyOrigin(h).ServeHTTP)
	mux.Handle("GET", "/_proxy/api/v1/documentation/{*path}", HandleAppsProxyOrigin(h).ServeHTTP)
}

// MountCORSHandler configures the mux to serve the CORS endpoints for the
// service apps-proxy.
func MountCORSHandler(mux goahttp.Muxer, h http.Handler) {
	h = HandleAppsProxyOrigin(h)
	mux.Handle("OPTIONS", "/_proxy/api/", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1/health-check", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1/validate", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1/documentation/openapi.json", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1/documentation/openapi.yaml", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1/documentation/openapi3.json", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1/documentation/openapi3.yaml", h.ServeHTTP)
	mux.Handle("OPTIONS", "/_proxy/api/v1/documentation/{*path}", h.ServeHTTP)
}

// NewCORSHandler creates a HTTP handler which returns a simple 204 response.
func NewCORSHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
}

// HandleAppsProxyOrigin applies the CORS response headers corresponding to the
// origin for the service apps-proxy.
func HandleAppsProxyOrigin(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			// Not a CORS request
			h.ServeHTTP(w, r)
			return
		}
		if cors.MatchOrigin(origin, "*") {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			if acrm := r.Header.Get("Access-Control-Request-Method"); acrm != "" {
				// We are handling a preflight request
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-StorageApi-Token")
				w.WriteHeader(204)
				return
			}
			h.ServeHTTP(w, r)
			return
		}
		h.ServeHTTP(w, r)
		return
	})
}