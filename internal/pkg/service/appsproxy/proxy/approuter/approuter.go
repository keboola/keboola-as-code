package approuter

import (
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	config      config.Config
	appHandlers *apphandler.Manager
	pageWriter  *pagewriter.Writer
}

type dependencies interface {
	Config() config.Config
	AppHandlers() *apphandler.Manager
	PageWriter() *pagewriter.Writer
}

func New(d dependencies) *Router {
	return &Router{
		config:      d.Config(),
		appHandlers: d.AppHandlers(),
		pageWriter:  d.PageWriter(),
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	appHandler := r.appHandlers.HandlerFor(req.Context())
	if appHandler != nil {
		appHandler.ServeHTTP(w, req)
		return
	}

	// Health check is served only if there is not appID
	if req.URL.Path == "/health-check" {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "OK")
		return
	}

	r.pageWriter.WriteError(w, req, nil, svcErrors.NewBadRequestError(errors.Errorf(`unexpected domain, missing application ID`)))
}
