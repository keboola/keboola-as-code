package apphandler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	config           config.Config
	telemetry        telemetry.Telemetry
	configLoader     appconfig.Loader
	upstreamManager  *upstream.Manager
	authProxyManager *authproxy.Manager
	pageWriter       *pagewriter.Writer
	handlers         *syncmap.SyncMap[api.AppID, appHandlerWrapper]
}

type appHandlerWrapper struct {
	lock        *sync.Mutex
	handler     http.Handler
	cancel      context.CancelCauseFunc
	handlerHash string // hash of UpstreamTarget + E2BAccessToken; handler is recreated when it changes
}

type dependencies interface {
	Config() config.Config
	Telemetry() telemetry.Telemetry
	PageWriter() *pagewriter.Writer
	UpstreamManager() *upstream.Manager
	AuthProxyManager() *authproxy.Manager
	AppConfigLoader() appconfig.Loader
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		config:           d.Config(),
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

func (m *Manager) HandlerFor(ctx context.Context, result appconfig.AppConfigResult) http.Handler {
	wrapper := m.handlers.GetOrInit(result.AppID)

	// Only one newHandler method runs in parallel per app.
	// If there is an in-flight update, we are waiting for its results.
	wrapper.lock.Lock()
	defer wrapper.lock.Unlock()

	// Load configuration for the app
	if result.Err != nil {
		return m.newErrorHandler(ctx, api.AppConfig{ID: result.AppID}, result.Err)
	}

	// Create a new handler when config changed, upstream URL changed, or E2B token changed.
	// Only a hash is stored so raw secrets don't linger in the wrapper.
	currentHash := handlerHash(m.upstreamManager.AppInfo(ctx, result.AppID))
	if wrapper.handler == nil || result.Modified || wrapper.handlerHash != currentHash {
		if wrapper.cancel != nil {
			wrapper.cancel(errors.New("configuration changed"))
		}
		wrapper.handler, wrapper.cancel = m.newHandler(ctx, result.AppConfig)
		wrapper.handlerHash = currentHash
	}

	return wrapper.handler
}

func (m *Manager) newHandler(ctx context.Context, app api.AppConfig) (http.Handler, context.CancelCauseFunc) {
	// Create upstream reverse proxy without authentication
	appUpstream, err := m.upstreamManager.NewUpstream(ctx, app)
	if err != nil {
		return m.newErrorHandler(ctx, app, err), nil
	}

	// Create authentication handlers
	authHandlers := m.authProxyManager.NewHandlers(app, appUpstream)

	// Create root handler for application
	handler, err := newAppHandler(m, app, appUpstream, authHandlers)
	if err != nil {
		err = svcErrors.NewServiceUnavailableError(errors.NewNestedError(
			errors.Errorf(`application "%s" has invalid configuration`, app.IdAndName()),
			err,
		))
		return m.newErrorHandler(ctx, app, err), nil
	}

	return handler, appUpstream.Cancel
}

// handlerHash computes a hash from the fields that require handler recreation when they change.
// Only a hash is stored so raw secrets don't linger in the wrapper.
func handlerHash(info k8sapp.AppInfo, ok bool) string {
	if !ok {
		return ""
	}
	h := sha256.New()
	if info.UpstreamTarget != nil {
		h.Write([]byte(info.UpstreamTarget.String()))
	}
	h.Write([]byte{0})
	h.Write([]byte(info.E2BAccessToken))
	return hex.EncodeToString(h.Sum(nil))
}

func (m *Manager) newErrorHandler(ctx context.Context, app api.AppConfig, err error) http.Handler {
	err = svcErrors.WrapWithExceptionID(middleware.RequestIDFromContext(ctx), err)
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		m.pageWriter.WriteError(w, req, &app, err)
	})
}
