package authproxy

import (
	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/basicauth"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oidcproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

type Manager struct {
	logger           log.Logger
	config           config.Config
	pageWriter       *pagewriter.Writer
	clock            clockwork.Clock
	providerSelector *selector.Selector
}

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Config() config.Config
	PageWriter() *pagewriter.Writer
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		logger:           d.Logger(),
		config:           d.Config(),
		pageWriter:       d.PageWriter(),
		clock:            d.Clock(),
		providerSelector: selector.New(d),
	}
}

func (m *Manager) ProviderSelector() *selector.Selector {
	return m.providerSelector
}

func (m *Manager) NewHandlers(app api.AppConfig, upstream chain.Handler) map[provider.ID]selector.Handler {
	authHandlers := make(map[provider.ID]selector.Handler, len(app.AuthProviders))
	for _, auth := range app.AuthProviders {
		switch p := auth.(type) {
		case provider.OIDC:
			authHandlers[auth.ID()] = oidcproxy.NewHandler(m.logger, m.config, m.providerSelector, m.pageWriter, app, p, upstream)

		case provider.Basic:
			authHandlers[auth.ID()] = basicauth.NewHandler(m.logger, m.config, m.clock, m.pageWriter, app, p, upstream)

		default:
			panic("unknown auth provider type")
		}
	}
	return authHandlers
}
