package oidcproxy

import (
	"fmt"
	"net/http"
	"time"

	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Handler struct {
	providerName string
	proxyConfig  *proxyOptions.Options
	proxyHandler *oauthproxy.OAuthProxy
	initErr      error
}

func NewHandler(
	logger log.Logger,
	cfg config.Config,
	selector *selector.Selector,
	pw *pagewriter.Writer,
	app api.AppConfig,
	auth provider.OIDC,
	upstream chain.Handler,
) *Handler {
	var err error
	handler := &Handler{providerName: auth.Name()}

	// Create proxy configuration
	handler.proxyConfig, err = proxyConfig(cfg, selector, pw, app, auth, upstream)
	if err != nil {
		handler.initErr = wrapHandlerInitErr(app, auth, err)
		return handler
	}

	// Create proxy page writer adapter
	pageWriter, err := newPageWriter(logger, pw, app, auth, handler.proxyConfig)
	if err != nil {
		handler.initErr = wrapHandlerInitErr(app, auth, err)
		return handler
	}

	// Create proxy HTTP handler
	authValidator := func(email string) bool { return true } // there is no need to verify individual users
	handler.proxyHandler, err = oauthproxy.NewOAuthProxyWithPageWriter(handler.proxyConfig, authValidator, pageWriter)
	if err != nil {
		handler.initErr = wrapHandlerInitErr(app, auth, err)
		return handler
	}

	return handler
}

func (h *Handler) Name() string {
	return h.providerName
}

func (h *Handler) CookieExpiration() time.Duration {
	if h.initErr != nil {
		return 5 * time.Minute
	}
	return h.proxyConfig.Cookie.Expire
}

func (h *Handler) SignInPath() string {
	if h.initErr != nil {
		return "/error"
	}
	return h.proxyHandler.SignInPath
}

func (h *Handler) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	if h.initErr != nil {
		return h.initErr
	}

	// Pass request to OAuth2Proxy
	h.proxyHandler.ServeHTTP(w, req) // errors are handled by the page writer
	return nil
}

func wrapHandlerInitErr(app api.AppConfig, auth provider.Provider, err error) error {
	return svcErrors.
		NewServiceUnavailableError(errors.PrefixErrorf(err, `application "%s" has invalid configuration for authentication provider "%s"`, app.IdAndName(), auth.ID())).
		WithUserMessage(fmt.Sprintf(`Application "%s" has invalid configuration for authentication provider "%s".`, app.IdAndName(), auth.ID()))
}
