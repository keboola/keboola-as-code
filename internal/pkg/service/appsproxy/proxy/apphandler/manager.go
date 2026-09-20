package apphandler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sync"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	logger               log.Logger
	config               config.Config
	telemetry            telemetry.Telemetry
	configLoader         appconfig.Loader
	upstreamManager      *upstream.Manager
	authProxyManager     *authproxy.Manager
	pageWriter           *pagewriter.Writer
	handlers             *syncmap.SyncMap[k8sapp.WorkloadRef, appHandlerWrapper]
	clock                clockwork.Clock
	storageTokenVerifier kaipreview.StorageTokenVerifier
	sessionsManager      *sessions.Manager
}

type appHandlerWrapper struct {
	lock        *sync.Mutex
	evicted     bool // the workload is gone and this entry has left the cache; never build into it again
	handler     http.Handler
	cancel      context.CancelCauseFunc
	handlerHash string // hash of UpstreamTarget + E2BAccessToken; handler is recreated when it changes
	configETag  string // ETag of the app config the handler was built from; handler is recreated when it changes
}

// needsRebuild reports whether the cached handler must be recreated.
// The handler is keyed on config identity (the config ETag) and the upstream
// hash, so any config change is picked up on the next request without relying
// on a one-shot "modified" signal from the config loader.
func (w *appHandlerWrapper) needsRebuild(configETag, currentHash string) bool {
	return w.handler == nil || w.configETag != configETag || w.handlerHash != currentHash
}

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Config() config.Config
	Telemetry() telemetry.Telemetry
	PageWriter() *pagewriter.Writer
	UpstreamManager() *upstream.Manager
	AuthProxyManager() *authproxy.Manager
	AppConfigLoader() appconfig.Loader
	SessionsManager() *sessions.Manager
	AppStateWatcher() *k8sapp.StateWatcher
}

func NewManager(ctx context.Context, d dependencies) (*Manager, error) {
	cfg := d.Config()
	if cfg.StorageAPIURL == nil {
		return nil, errors.New("appsproxy: StorageAPIURL is required for kai-preview Storage token verification")
	}
	verifier, err := kaipreview.NewSDKStorageTokenVerifier(ctx, cfg.StorageAPIURL.String())
	if err != nil {
		return nil, err
	}
	m := &Manager{
		logger:           d.Logger(),
		config:           cfg,
		telemetry:        d.Telemetry(),
		configLoader:     d.AppConfigLoader(),
		upstreamManager:  d.UpstreamManager(),
		authProxyManager: d.AuthProxyManager(),
		pageWriter:       d.PageWriter(),
		handlers: syncmap.New[k8sapp.WorkloadRef, appHandlerWrapper](func(k8sapp.WorkloadRef) *appHandlerWrapper {
			return &appHandlerWrapper{lock: &sync.Mutex{}}
		}),
		clock:                d.Clock(),
		storageTokenVerifier: verifier,
		sessionsManager:      d.SessionsManager(),
	}

	d.AppStateWatcher().OnWorkloadRemoved(m.evictWorkload)

	return m, nil
}

// evictWorkload drops the cached handler for a workload that no longer exists.
// The cache is keyed by workload and a draft is short-lived, so without this it
// grows with every draft ever served and strands each handler's reverse proxy
// and its uncancelled context.
func (m *Manager) evictWorkload(ref k8sapp.WorkloadRef) {
	wrapper, ok := m.handlers.Delete(ref)
	if !ok {
		return
	}

	wrapper.lock.Lock()
	defer wrapper.lock.Unlock()
	wrapper.evicted = true
	if wrapper.cancel != nil {
		wrapper.cancel(errors.New("workload removed"))
		wrapper.cancel = nil
	}
	wrapper.handler = nil
}

func (m *Manager) HandlerFor(ctx context.Context, result appconfig.AppConfigResult) http.Handler {
	// The entry can be evicted between taking the pointer and taking its lock,
	// in which case it is no longer the cache's and must not be built into.
	for {
		if handler, ok := m.handlerFor(ctx, result, m.handlers.GetOrInit(result.Workload)); ok {
			return handler
		}
	}
}

func (m *Manager) handlerFor(ctx context.Context, result appconfig.AppConfigResult, wrapper *appHandlerWrapper) (http.Handler, bool) {
	// Only one newHandler method runs in parallel per app.
	// If there is an in-flight update, we are waiting for its results.
	wrapper.lock.Lock()
	defer wrapper.lock.Unlock()

	if wrapper.evicted {
		return nil, false
	}

	// Load configuration for the app
	if result.Err != nil {
		return m.newErrorHandler(ctx, api.AppConfig{ID: result.Workload.AppID}, result.Err), true
	}

	// Create a new handler when the config changed (ETag), upstream URL changed, or E2B token changed.
	// Only a hash is stored so raw secrets don't linger in the wrapper.
	currentHash := handlerHash(m.upstreamManager.AppInfo(ctx, result.Workload))
	configETag := result.AppConfig.ETag()
	if wrapper.needsRebuild(configETag, currentHash) {
		if wrapper.cancel != nil {
			wrapper.cancel(errors.New("configuration changed"))
		}
		wrapper.handler, wrapper.cancel = m.newHandler(ctx, result.AppConfig, result.Workload)
		wrapper.handlerHash = currentHash
		wrapper.configETag = configETag
	}

	return wrapper.handler, true
}

func (m *Manager) newHandler(ctx context.Context, app api.AppConfig, workload k8sapp.WorkloadRef) (http.Handler, context.CancelCauseFunc) {
	// Create upstream reverse proxy without authentication
	appUpstream, err := m.upstreamManager.NewUpstream(ctx, app, workload)
	if err != nil {
		return m.newErrorHandler(ctx, app, err), nil
	}

	// Track the end-user session. Sits between authentication and the upstream:
	// the X-Kbc-User-* headers are already injected at this point, while paths
	// that require no authentication still get an anonymous session.
	trackedUpstream := chain.New(appUpstream).Prepend(m.sessionsManager.Middleware(app, workload))

	// Create authentication handlers
	authHandlers := m.authProxyManager.NewHandlers(app, trackedUpstream)

	// Create root handler for application
	handler, err := newAppHandler(m, app, workload, trackedUpstream, authHandlers)
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
	h.Write([]byte(info.PublicHost))
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
