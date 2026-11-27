package apphandler

import (
	"context"
	"net/http"
	"sync"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	logger           log.Logger
	config           config.Config
	telemetry        telemetry.Telemetry
	configLoader     appconfig.Loader
	upstreamManager  *upstream.Manager
	authProxyManager *authproxy.Manager
	pageWriter       *pagewriter.Writer
	handlers         *syncmap.SyncMap[handlerKey, appHandlerWrapper]
	sessionsManager  *sessions.Manager
	appTokenMinter   *apptoken.Minter
	preview          *preview.Service
	customHosts      appconfig.CustomHostResolver
}

// handlerKey identifies a cached handler: one per workload and public host.
// Host is empty on the hub host. WorkloadRef stays host-free, so the state, wakeup and idle-timer lookups are shared by all hosts.
type handlerKey struct {
	Workload k8sapp.WorkloadRef
	Host     string
}

// normalizeResult makes the app config's custom host the normalized index key, so every request spelling shares one handler.
func normalizeResult(result appconfig.AppConfigResult) appconfig.AppConfigResult {
	if result.AppConfig.CustomHost != "" {
		result.AppConfig.CustomHost = k8sapp.NormalizeHost(result.AppConfig.CustomHost)
	}
	return result
}

func handlerKeyFor(result appconfig.AppConfigResult) handlerKey {
	return handlerKey{Workload: result.Workload, Host: result.AppConfig.CustomHost}
}

type appHandlerWrapper struct {
	lock       *sync.Mutex
	evicted    bool // the workload is gone and this entry has left the cache; never build into it again
	handler    http.Handler
	cancel     context.CancelCauseFunc
	upstream   upstreamKey // handler is recreated when it changes
	configETag string      // ETag of the app config the handler was built from; handler is recreated when it changes
}

// needsRebuild reports whether the cached handler must be recreated.
// The handler is keyed on config identity (the config ETag) and the upstream
// key, so any config change is picked up on the next request without relying
// on a one-shot "modified" signal from the config loader.
func (w *appHandlerWrapper) needsRebuild(configETag string, upstream upstreamKey) bool {
	return w.handler == nil || w.configETag != configETag || w.upstream != upstream
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
	AppTokenMinter() *apptoken.Minter
	AppStateWatcher() *k8sapp.StateWatcher
	CustomHosts() *k8sapp.CustomHosts
	Process() *servicectx.Process
}

func NewManager(d dependencies) *Manager {
	cfg := d.Config()
	m := &Manager{
		logger:           d.Logger(),
		config:           cfg,
		telemetry:        d.Telemetry(),
		configLoader:     d.AppConfigLoader(),
		upstreamManager:  d.UpstreamManager(),
		authProxyManager: d.AuthProxyManager(),
		pageWriter:       d.PageWriter(),
		handlers: syncmap.New[handlerKey, appHandlerWrapper](func(handlerKey) *appHandlerWrapper {
			return &appHandlerWrapper{lock: &sync.Mutex{}}
		}),
		sessionsManager: d.SessionsManager(),
		appTokenMinter:  d.AppTokenMinter(),
		customHosts:     d.CustomHosts(),
	}

	if cfg.Preview.Enabled() {
		m.preview = preview.New(d, cfg.Preview)
	}

	d.AppStateWatcher().OnWorkloadRemoved(m.evictWorkload)
	d.CustomHosts().OnHostRemoved(m.EvictHost)

	return m
}

// evictWorkload drops the cached handler for a workload that no longer exists.
// The cache is keyed by workload and host, and a draft is short-lived, so without this it
// grows with every draft ever served and strands each handler's reverse proxy
// and its uncancelled context.
func (m *Manager) evictWorkload(ref k8sapp.WorkloadRef) {
	m.handlers.Range(func(key handlerKey, _ *appHandlerWrapper) {
		if key.Workload == ref {
			m.evict(key, "workload removed")
		}
	})
}

// EvictHost drops the cached handler a workload serves on one custom host.
func (m *Manager) EvictHost(ref k8sapp.WorkloadRef, host string) {
	m.evict(handlerKey{Workload: ref, Host: host}, "host removed")
}

func (m *Manager) evict(key handlerKey, reason string) {
	wrapper, ok := m.handlers.Delete(key)
	if !ok {
		return
	}

	wrapper.lock.Lock()
	defer wrapper.lock.Unlock()
	wrapper.evicted = true
	if wrapper.cancel != nil {
		wrapper.cancel(errors.New(reason))
		wrapper.cancel = nil
	}
	wrapper.handler = nil
}

func (m *Manager) HandlerFor(ctx context.Context, result appconfig.AppConfigResult) http.Handler {
	result = normalizeResult(result)
	// An unrouted host builds no handler, so a request cannot repopulate an entry the eviction just removed.
	if !m.hostRouted(result) {
		return m.newErrorHandler(ctx, api.AppConfig{ID: result.Workload.AppID}, hostNotRoutedError(result))
	}
	return m.handlerFor(ctx, result, m.handlers.GetOrInit(handlerKeyFor(result)))
}

// hostRouted reports whether the custom host of the request still routes to its workload. The index is updated
// before the eviction callback runs, so a check made under the wrapper lock cannot pass for a wrapper that the
// eviction missed.
func (m *Manager) hostRouted(result appconfig.AppConfigResult) bool {
	host := result.AppConfig.CustomHost
	if host == "" {
		return true
	}
	route, ok := m.customHosts.Resolve(host)
	return ok && route.Workload == result.Workload
}

func hostNotRoutedError(result appconfig.AppConfigResult) error {
	return svcErrors.NewResourceNotFoundError("host", result.AppConfig.CustomHost, "cluster")
}

func (m *Manager) handlerFor(ctx context.Context, result appconfig.AppConfigResult, wrapper *appHandlerWrapper) http.Handler {
	// Only one newHandler method runs in parallel per app.
	// If there is an in-flight update, we are waiting for its results.
	wrapper.lock.Lock()
	defer wrapper.lock.Unlock()

	// The entry can be evicted between taking the pointer and taking its lock.
	// Building into it would strand the handler, and the workload it serves is
	// gone anyway, so the request ends here.
	if wrapper.evicted {
		return m.newErrorHandler(ctx, api.AppConfig{ID: result.Workload.AppID}, svcErrors.NewResourceNotFoundError("workload", result.Workload.String(), "cluster"))
	}

	if !m.hostRouted(result) {
		m.handlers.DeleteIf(handlerKeyFor(result), wrapper)
		return m.newErrorHandler(ctx, api.AppConfig{ID: result.Workload.AppID}, hostNotRoutedError(result))
	}

	// Load configuration for the app
	if result.Err != nil {
		return m.newErrorHandler(ctx, api.AppConfig{ID: result.Workload.AppID}, result.Err)
	}

	info, cached := m.upstreamManager.AppInfo(result.Workload)
	if cached && info.NotDeployed {
		return m.newErrorHandler(ctx, api.AppConfig{ID: result.Workload.AppID}, svcErrors.NewResourceNotFoundError("application", result.Workload.AppID.String(), "stack"))
	}

	// Create a new handler when the config changed (ETag), or the upstream URL or public host changed.
	upstream := upstreamKeyFor(info, cached)
	configETag := result.AppConfig.ETag()
	if wrapper.needsRebuild(configETag, upstream) {
		if wrapper.cancel != nil {
			wrapper.cancel(errors.New("configuration changed"))
		}
		wrapper.handler, wrapper.cancel = m.newHandler(ctx, result.AppConfig, result.Workload)
		wrapper.upstream = upstream
		wrapper.configETag = configETag
	}

	return wrapper.handler
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
	// The app user token is injected after it, because the token carries the session id.
	trackedUpstream := chain.New(appUpstream).Prepend(
		rejectInternalPaths,
		m.sessionsManager.Middleware(app, workload),
		m.appTokenMinter.Inject(app, workload),
		stripProxyCookies,
	)

	// Create authentication handlers
	authHandlers := m.authProxyManager.NewHandlers(ctx, app, trackedUpstream)

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

// upstreamKey holds the workload fields that require handler recreation when they change.
type upstreamKey struct {
	cached     bool
	targetURL  string
	publicHost string
}

func upstreamKeyFor(info k8sapp.AppInfo, cached bool) upstreamKey {
	if !cached {
		return upstreamKey{}
	}
	key := upstreamKey{cached: true, publicHost: info.PublicHost}
	if info.UpstreamTarget != nil {
		key.targetURL = info.UpstreamTarget.String()
	}
	return key
}

func (m *Manager) newErrorHandler(ctx context.Context, app api.AppConfig, err error) http.Handler {
	err = svcErrors.WrapWithExceptionID(middleware.RequestIDFromContext(ctx), err)
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		m.pageWriter.WriteError(w, req, &app, err)
	})
}
