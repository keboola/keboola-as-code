package http

import (
	"net/http"
	"regexp"

	"github.com/dimfeld/httptreemux/v5"
	goaHTTP "goa.design/goa/v3/http"
)

type muxer struct {
	*httptreemux.ContextMux
}

// newMuxer returns a Muxer implementation with custom 404 not found error handler.
func newMuxer() goaHTTP.Muxer {
	r := httptreemux.NewContextMux()

	r.EscapeAddedRoutes = true
	r.NotFoundHandler = func(w http.ResponseWriter, req *http.Request) {
		_ = writeError(req.Context(), w, NotFoundError{})
	}
	r.PanicHandler = func(w http.ResponseWriter, req *http.Request, value interface{}) {
		_ = writeError(req.Context(), w, PanicError{Value: value})
	}
	return &muxer{r}
}

// Handle maps the wildcard format used by goa to the one used by httptreemux.
func (m *muxer) Handle(method, pattern string, handler http.HandlerFunc) {
	m.ContextMux.Handle(method, treemuxify(pattern), handler)
}

// Vars extracts the path variables from the request context.
func (m *muxer) Vars(r *http.Request) map[string]string {
	return httptreemux.ContextParams(r.Context())
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
