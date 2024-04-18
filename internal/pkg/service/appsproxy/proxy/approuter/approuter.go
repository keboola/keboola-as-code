package approuter

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/requtil"
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
	if appID, ok := r.parseAppID(req); ok {
		r.appHandlers.HandlerFor(req.Context(), appID).ServeHTTP(w, req)
		return
	}

	// Health check is served only if there is not appID
	if req.URL.Path == "/health-check" {
		_, _ = fmt.Fprintln(w, "OK")
		w.WriteHeader(http.StatusOK)
		return
	}

	r.pageWriter.WriteError(w, req, svcErrors.NewBadRequestError(errors.Errorf(`unexpected domain, missing application ID`)))
}

func (r *Router) parseAppID(req *http.Request) (api.AppID, bool) {
	// Request domain must match expected public domain
	domain := requtil.Host(req)
	if !strings.HasSuffix(domain, "."+r.config.API.PublicURL.Host) {
		return "", false
	}

	// Only one subdomain is allowed
	if strings.Count(domain, ".") != strings.Count(r.config.API.PublicURL.Host, ".")+1 {
		return "", false
	}

	// Get subdomain
	subdomain := domain[:strings.IndexByte(domain, '.')]

	// Remove optional app name prefix, if any
	lastDash := strings.LastIndexByte(subdomain, '-')
	if lastDash >= 0 {
		return api.AppID(subdomain[lastDash+1:]), true
	}

	return api.AppID(subdomain), true
}
