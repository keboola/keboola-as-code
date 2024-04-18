package apphandler

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"net/http"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	telemetry        telemetry.Telemetry
	configLoader     *appconfig.Loader
	upstreamManager  *upstream.Manager
	authProxyManager *authproxy.Manager
	pageWriter       *pagewriter.Writer
	handlers         *syncmap.SyncMap[api.AppID, appHandlerWrapper]
}

type appHandlerWrapper struct {
	lock    *sync.Mutex
	handler http.Handler
}

type dependencies interface {
	Telemetry() telemetry.Telemetry
	PageWriter() *pagewriter.Writer
	UpstreamManager() *upstream.Manager
	AuthProxyManager() *authproxy.Manager
	AppConfigLoader() *appconfig.Loader
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		telemetry:        d.Telemetry(),
		configLoader:     d.AppConfigLoader(),
		upstreamManager:  d.UpstreamManager(),
		authProxyManager: d.AuthProxyManager(),
		pageWriter:       d.PageWriter(),
		handlers: syncmap.New[api.AppID, appHandlerWrapper](func(api.AppID) *appHandlerWrapper {
			return &appHandlerWrapper{lock: &sync.Mutex{}}
		}),
	}
}

func (m *Manager) HandlerFor(ctx context.Context, appID api.AppID) http.Handler {
	wrapper := m.handlers.GetOrInit(appID)

	// Only one newHandler method runs in parallel per app.
	// If there is an in-flight update, we are waiting for its results.
	wrapper.lock.Lock()
	defer wrapper.lock.Unlock()

	// Load configuration for the app
	app, modified, err := m.configLoader.GetConfig(ctx, appID)
	if err != nil {
		return m.newErrorHandler(ctx, err)
	}

	// Create a new handler, if needed
	if wrapper.handler == nil || modified {
		wrapper.handler = m.newHandler(ctx, app)
	}

	return wrapper.handler
}

func (m *Manager) newHandler(ctx context.Context, app api.AppConfig) http.Handler {
	// Create upstream reverse proxy without authentication
	appUpstream, err := m.upstreamManager.NewUpstream(ctx, app)
	if err != nil {
		return m.newErrorHandler(ctx, err)
	}

	// Create authentication handlers
	authHandlers, err := m.authProxyManager.NewHandlers(app, appUpstream)
	if err != nil {
		return m.newErrorHandler(ctx, err)
	}

	// Create root handler for application
	handler, err := newAppHandler(m, app, appUpstream, authHandlers)
	if err != nil {
		err = svcErrors.NewServiceUnavailableError(errors.NewNestedError(
			errors.Errorf(`application "%s" has invalid configuration`, app.IdAndName()),
			err,
		))
		return m.newErrorHandler(ctx, err)
	}

	return handler
}

func (m *Manager) newErrorHandler(ctx context.Context, err error) http.Handler {
	err = svcErrors.WrapWithExceptionID(middleware.RequestIDFromContext(ctx), err)
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		m.pageWriter.WriteError(w, req, err)
	})
}
