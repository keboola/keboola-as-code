package authproxy

import (
	"context"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/redis/go-redis/v9"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/basicauth"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

type Manager struct {
	logger           log.Logger
	config           config.Config
	pageWriter       *pagewriter.Writer
	clock            clockwork.Clock
	httpClient       client.Client
	providerSelector *selector.Selector
	sessionStore     *redis.Client
}

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	HTTPClient() client.Client
	Config() config.Config
	PageWriter() *pagewriter.Writer
	SessionStoreClient() *redis.Client
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		logger:           d.Logger(),
		config:           d.Config(),
		pageWriter:       d.PageWriter(),
		clock:            d.Clock(),
		httpClient:       d.HTTPClient(),
		providerSelector: selector.New(d),
		sessionStore:     d.SessionStoreClient(),
	}
}

func (m *Manager) ProviderSelector() *selector.Selector {
	return m.providerSelector
}

func (m *Manager) NewHandlers(ctx context.Context, app api.AppConfig, upstream chain.Handler) map[provider.ID]selector.Handler {
	authHandlers := make(map[provider.ID]selector.Handler, len(app.AuthProviders))
	for _, auth := range app.AuthProviders {
		// Record which provider admitted the request, so a session event can
		// name it. oauth2-proxy does not pass this on, and one app may offer
		// several providers.
		providerUpstream := sessions.WithAuthProvider(upstream, auth.ID(), auth.Type())
		switch p := auth.(type) {
		case provider.OAuthProvider:
			authHandlers[auth.ID()] = oauthproxy.NewHandler(ctx, m.logger, m.httpClient, m.config, m.sessionStore, m.providerSelector, m.pageWriter, app, p, apptoken.WithIdentity(providerUpstream, p.IdentityProviderURL()))
		case provider.Basic:
			authHandlers[auth.ID()] = basicauth.NewHandler(m.logger, m.config, m.clock, m.pageWriter, app, p, providerUpstream)
		default:
			panic("unknown auth provider type")
		}
	}
	return authHandlers
}
