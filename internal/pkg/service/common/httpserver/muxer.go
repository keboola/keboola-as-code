package httpserver

import (
	"net/http"
	"regexp"

	"github.com/dimfeld/httptreemux/v5"
	goaHTTP "goa.design/goa/v3/http"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

type Muxer interface {
	goaHTTP.MiddlewareMuxer
}

type muxer struct {
	*httptreemux.ContextMux
}

// NewMuxer returns a Muxer implementation with custom not found and panic error handlers.
func NewMuxer(errorWriter ErrorWriter) Muxer {
	mux := httptreemux.NewContextMux()
	mux.NotFoundHandler = func(w http.ResponseWriter, req *http.Request) {
		errorWriter.WriteWithStatusCode(req.Context(), w, serviceError.NewEndpointNotFoundError(req.URL))
	}
	mux.PanicHandler = func(w http.ResponseWriter, req *http.Request, value any) {
		errorWriter.WriteWithStatusCode(req.Context(), w, serviceError.NewPanicError(value))
	}
	return &muxer{ContextMux: mux}
}

// Handle maps the wildcard format used by goa to the one used by httptreemux.
func (m *muxer) Handle(method, pattern string, handler http.HandlerFunc) {
	m.ContextMux.Handle(method, treemuxify(pattern), handler)
}

// Vars extracts the path variables from the request context.
func (m *muxer) Vars(r *http.Request) map[string]string {
	return httptreemux.ContextParams(r.Context())
}

// Use appends a middleware to the list of middlewares to be applied to the Muxer.
func (m *muxer) Use(fn func(http.Handler) http.Handler) {
	m.UseHandler(fn)
}

var (
	wildSeg  = regexp.MustCompile(`/{([a-zA-Z0-9_]+)}`)
	wildPath = regexp.MustCompile(`/{\*([a-zA-Z0-9_]+)}`)
)

func treemuxify(pattern string) string {
	pattern = wildSeg.ReplaceAllString(pattern, "/:$1")
	pattern = wildPath.ReplaceAllString(pattern, "/*$1")
	return pattern
}
