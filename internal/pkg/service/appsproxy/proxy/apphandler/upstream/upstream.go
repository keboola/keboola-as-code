// Package upstream provides HTTP and WebSocket reverse proxy to data apps without authentication.
package upstream

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/wakeup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	notifyRequestTimeout = 5 * time.Second
	wakeupRequestTimeout = 60 * time.Second
	attrWakeupReason     = "proxy.wakeup.reason"
	attrWebsocket        = "proxy.websocket"
)

type Manager struct {
	wg           *sync.WaitGroup
	logger       log.Logger
	telemetry    telemetry.Telemetry
	transport    http.RoundTripper
	pageWriter   *pagewriter.Writer
	configLoader appconfig.Loader
	notify       *notify.Manager
	wakeup       *wakeup.Manager
	stateWatcher *k8sapp.StateWatcher
	config       config.Config
}

type AppUpstream struct {
	manager       *Manager
	app           api.AppConfig
	target        *url.URL // parsed from appsProxy.upstreamUrl at creation; nil when absent
	handler       *chain.Chain
	wsHandler     *chain.Chain
	cancelWs      context.CancelCauseFunc
	activeWsCount atomic.Int64
}

type dependencies interface {
	Process() *servicectx.Process
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	UpstreamTransport() http.RoundTripper
	PageWriter() *pagewriter.Writer
	AppConfigLoader() appconfig.Loader
	NotifyManager() *notify.Manager
	WakeupManager() *wakeup.Manager
	AppStateWatcher() *k8sapp.StateWatcher
	Config() config.Config
}

func NewManager(d dependencies) *Manager {
	m := &Manager{
		wg:           &sync.WaitGroup{},
		logger:       d.Logger().WithComponent("upstream"),
		telemetry:    d.Telemetry(),
		transport:    d.UpstreamTransport(),
		pageWriter:   d.PageWriter(),
		configLoader: d.AppConfigLoader(),
		notify:       d.NotifyManager(),
		wakeup:       d.WakeupManager(),
		stateWatcher: d.AppStateWatcher(),
		config:       d.Config(),
	}

	d.Process().OnShutdown(func(ctx context.Context) {
		m.Shutdown(ctx)
	})

	return m
}

func (m *Manager) Shutdown(ctx context.Context) {
	m.logger.Infof(ctx, `waiting for running notify/wakeup requests`)
	m.wg.Wait()
}

// CurrentServiceRef returns the appsProxy.upstreamUrl string for appID from the K8s cache.
// Returns "" when the app is not cached or the field is absent/invalid.
func (m *Manager) CurrentServiceRef(appID api.AppID) string {
	info, ok := m.stateWatcher.GetState(appID)
	if !ok || info.UpstreamTarget == nil {
		return ""
	}
	return info.UpstreamTarget.String()
}

func (m *Manager) NewUpstream(ctx context.Context, app api.AppConfig) (upstream *AppUpstream, err error) {
	_, span := m.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.upstream.NewUpstream")
	defer span.End(&err)

	// Resolve target URL at creation time; immutable after this point.
	var target *url.URL
	if info, ok := m.stateWatcher.GetState(app.ID); ok {
		target = info.UpstreamTarget // pre-parsed by watcher on CRD event; may be nil
	}

	// Create reverse proxy
	upstream = &AppUpstream{manager: m, app: app, target: target}
	upstream.handler = upstream.newProxy(m.config.Upstream.HTTPTimeout)
	upstream.wsHandler = upstream.newWebsocketProxy(m.config.Upstream.WsTimeout)

	// Call notify while there is an active websocket connection
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if upstream.activeWsCount.Load() > 0 {
					upstream.notify(ctx)
				}
				time.Sleep(30 * time.Second)
			}
		}
	}(ctx)

	return upstream, nil
}

func (u *AppUpstream) ServeHTTPOrError(rw http.ResponseWriter, req *http.Request) error {
	ctx := req.Context()

	// K8s state pre-check: if we know the app is not running, handle it synchronously
	// without attempting DNS/upstream. Falls through if state is unknown or Running.
	appInfo, ok := u.manager.stateWatcher.GetState(u.app.ID)
	if ok {
		u.manager.logger.Debugf(ctx, "app %q state check: actualState=%q autoRestartEnabled=%v", u.app.ID, appInfo.ActualState, appInfo.AutoRestartEnabled)
	} else {
		u.manager.logger.Debugf(ctx, "app %q state check: not in cache, forwarding to upstream", u.app.ID)
	}
	if ok && appInfo.ActualState != k8sapp.AppActualStateRunning {
		if appInfo.ActualState == k8sapp.AppActualStateStarting {
			u.manager.logger.Debugf(ctx, "app %q is starting, serving spinner page", u.app.ID)
			u.manager.pageWriter.WriteSpinnerPage(rw, req, u.app)
			return nil
		}
		if !appInfo.AutoRestartEnabled {
			u.manager.logger.Debugf(ctx, "app %q is not running and restart is disabled, serving restart-disabled page", u.app.ID)
			u.manager.pageWriter.WriteRestartDisabledPage(rw, req, u.app)
			return nil
		}
		u.manager.logger.Debugf(ctx, "app %q is not running, triggering wakeup and serving spinner page", u.app.ID)
		u.wakeup(ctx, errors.Errorf("app state is %s", appInfo.ActualState))
		u.manager.pageWriter.WriteSpinnerPage(rw, req, u.app)
		return nil
	}

	// Target set at creation time; nil means appsProxy.upstreamUrl was absent.
	if u.target == nil {
		u.manager.logger.Debugf(ctx, "app %q has no appsProxy.upstreamUrl, serving spinner page", u.app.ID)
		u.manager.pageWriter.WriteSpinnerPage(rw, req, u.app)
		return nil
	}

	// Difference between regular and websocket request
	if strings.EqualFold(req.Header.Get("Connection"), "upgrade") && req.Header.Get("Upgrade") == "websocket" {
		return u.wsHandler.ServeHTTPOrError(rw, req)
	}
	return u.handler.ServeHTTPOrError(rw, req)
}

func (u *AppUpstream) newReverseProxy() *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(u.target)
	proxy.Transport = u.manager.transport
	proxy.ErrorHandler = u.manager.pageWriter.ProxyErrorHandlerFor(u.app)

	// Clear req.Host so Go's HTTP client derives the Host header from req.URL.Host.
	// httputil.NewSingleHostReverseProxy rewrites req.URL.Host but leaves req.Host
	// (the actual Host header) set to the original incoming value, which causes
	// upstreams that route by Host to return wrong responses.
	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Host = ""
	}
	return proxy
}

func (u *AppUpstream) newProxy(timeout time.Duration) *chain.Chain {
	proxy := u.newReverseProxy()

	return chain.
		New(chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
			ctx := ctxattr.ContextWith(req.Context(), attribute.Bool(attrWebsocket, false))
			ctx, cancel := context.WithTimeoutCause(ctx, timeout, errors.New("upstream request timeout"))
			defer cancel()
			proxy.ServeHTTP(w, req.WithContext(ctx))
			return nil
		})).
		Prepend(
			// Trace connection events
			u.trace(),
		)
}

func (u *AppUpstream) newWebsocketProxy(timeout time.Duration) *chain.Chain {
	proxy := u.newReverseProxy()

	return chain.
		New(chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
			ctx := ctxattr.ContextWith(req.Context(), attribute.Bool(attrWebsocket, true))
			ctx, cancel := context.WithTimeoutCause(ctx, timeout, errors.New("upstream websocket request timeout"))
			defer cancel()

			ctx, c := context.WithCancelCause(ctx)
			u.cancelWs = c

			u.activeWsCount.Add(1)
			defer u.activeWsCount.Add(-1)

			proxy.ServeHTTP(w, req.WithContext(ctx))
			return nil
		})).
		Prepend(
			// Trace connection events
			u.trace(),
		)
}

func (u *AppUpstream) trace() chain.Middleware {
	return func(next chain.Handler) chain.Handler {
		return chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
			ctx := req.Context()

			// Trace connection events
			reqCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				GotConn: func(connInfo httptrace.GotConnInfo) {
					u.notify(ctx)
				},
			})

			return next.ServeHTTPOrError(w, req.WithContext(reqCtx))
		})
	}
}

func (u *AppUpstream) notify(ctx context.Context) {
	// The request should not wait for the notification
	u.manager.wg.Go(func() {
		notificationCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), notifyRequestTimeout, errors.New("upstream notification timeout"))
		defer cancel()

		_, span := u.manager.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.upstream.notify")
		notificationCtx = telemetry.ContextWithSpan(notificationCtx, span)

		// Error is already logged by the Notify method itself.
		err := u.manager.notify.Notify(notificationCtx, u.app.ID) //nolint:contextcheck
		span.End(&err)
	})
}

func (u *AppUpstream) wakeup(ctx context.Context, err error) {
	// The request should not wait for the wakeup request
	u.manager.wg.Go(func() {
		wakeupCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), wakeupRequestTimeout, errors.New("upstream wakeup timeout"))
		defer cancel()

		_, span := u.manager.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.upstream.wakeup")
		span.SetAttributes(attribute.String(attrWakeupReason, err.Error()))
		wakeupCtx = telemetry.ContextWithSpan(wakeupCtx, span)

		// Error is already logged by the Wakeup method itself.
		err := u.manager.wakeup.Wakeup(wakeupCtx, u.app.ID) //nolint:contextcheck
		span.End(&err)
	})
}

func (u *AppUpstream) Cancel(err error) {
	if u.cancelWs != nil {
		u.cancelWs(err)
	}
}
