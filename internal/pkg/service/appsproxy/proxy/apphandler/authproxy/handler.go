package authproxy

import (
	"fmt"
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	config           config.Config
	pageWriter       *pagewriter.Writer
	providerSelector *Selector
}

type Handler struct {
	provider     provider.Provider
	proxyConfig  *proxyOptions.Options
	proxyHandler *oauthproxy.OAuthProxy
	initErr      error
}

type dependencies interface {
	Clock() clock.Clock
	Config() config.Config
	PageWriter() *pagewriter.Writer
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		config:           d.Config(),
		pageWriter:       d.PageWriter(),
		providerSelector: newSelector(d),
	}
}

func (m *Manager) ProviderSelector() *Selector {
	return m.providerSelector
}

func (m *Manager) NewHandlers(app api.AppConfig, upstream chain.Handler) (map[provider.ID]*Handler, error) {
	out := make(map[provider.ID]*Handler, len(app.AuthProviders))
	for _, auth := range app.AuthProviders {
		out[auth.ID()] = m.newHandler(app, auth, upstream)
	}
	return out, nil
}

func (m *Manager) newHandler(app api.AppConfig, auth provider.Provider, upstream chain.Handler) *Handler {
	var err error
	handler := &Handler{provider: auth}

	// Create proxy configuration
	handler.proxyConfig, err = m.proxyConfig(app, auth, upstream)
	if err != nil {
		handler.initErr = wrapHandlerInitErr(app, auth, err)
		return handler
	}

	// Create proxy page writer adapter
	pw, err := m.newPageWriter(app, auth, handler.proxyConfig)
	if err != nil {
		handler.initErr = wrapHandlerInitErr(app, auth, err)
		return handler
	}

	// Create proxy HTTP handler
	authValidator := func(email string) bool { return true } // there is no need to verify individual users
	handler.proxyHandler, err = oauthproxy.NewOAuthProxyWithPageWriter(handler.proxyConfig, authValidator, pw)
	if err != nil {
		handler.initErr = wrapHandlerInitErr(app, auth, err)
		return handler
	}

	return handler
}

func (h *Handler) ID() provider.ID {
	return h.provider.ID()
}

func (h *Handler) Name() string {
	return h.provider.Name()
}

func (h *Handler) Provider() provider.Provider {
	return h.provider
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

	h.proxyHandler.ServeHTTP(w, req) // errors are handled by the page writer
	return nil
}

func wrapHandlerInitErr(app api.AppConfig, auth provider.Provider, err error) error {
	return svcErrors.
		NewServiceUnavailableError(errors.PrefixErrorf(err, `application "%s" has invalid configuration for authentication provider "%s"`, app.IdAndName(), auth.ID())).
		WithUserMessage(fmt.Sprintf(`Application "%s" has invalid configuration for authentication provider "%s".`, app.IdAndName(), auth.ID()))
}
