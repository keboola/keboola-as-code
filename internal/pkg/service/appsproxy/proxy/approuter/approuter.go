package approuter

import (
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const jwksPath = "/.well-known/jwks.json"

type Router struct {
	config         config.Config
	appHandlers    *apphandler.Manager
	pageWriter     *pagewriter.Writer
	appTokenMinter *apptoken.Minter
}

type dependencies interface {
	Config() config.Config
	AppHandlers() *apphandler.Manager
	PageWriter() *pagewriter.Writer
	AppTokenMinter() *apptoken.Minter
}

func New(d dependencies) *Router {
	return &Router{
		config:         d.Config(),
		appHandlers:    d.AppHandlers(),
		pageWriter:     d.PageWriter(),
		appTokenMinter: d.AppTokenMinter(),
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	result := appconfig.AppConfigFromContext(ctx)
	if result.Workload.AppID != "" {
		r.appHandlers.HandlerFor(ctx, result).ServeHTTP(w, req)
		return
	}

	// Health check is served only if there is not appID
	if req.URL.Path == "/health-check" {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "OK")
		return
	}

	if req.URL.Path == jwksPath {
		w.Header().Set("Content-Type", "application/jwk-set+json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, _ = w.Write(r.appTokenMinter.JWKS()) //nolint:gosec // G705: a JSON body built from the configured public keys, not from the request
		return
	}

	r.pageWriter.WriteError(w, req, nil, svcErrors.NewBadRequestError(errors.Errorf(`unexpected domain, missing application ID`)))
}
