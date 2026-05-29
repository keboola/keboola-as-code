// Package upstream provides HTTP and WebSocket reverse proxy to data apps without authentication.
package upstream

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
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
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream/wsactivity"
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
	manager   *Manager
	app       api.AppConfig
	target    *url.URL // parsed from appsProxy.upstreamUrl at creation; nil when absent
	handler   *chain.Chain
	wsHandler *chain.Chain
	cancelWs  context.CancelCauseFunc
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

// AppInfo returns the cached AppInfo for appID from the K8s cache in a single call.
// Returns (AppInfo{}, false) when the app is not cached.
func (m *Manager) AppInfo(ctx context.Context, appID api.AppID) (k8sapp.AppInfo, bool) {
	return m.stateWatcher.GetState(ctx, appID)
}

func (m *Manager) NewUpstream(ctx context.Context, app api.AppConfig) (upstream *AppUpstream, err error) {
	_, span := m.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.upstream.NewUpstream")
	defer span.End(&err)

	// Resolve target URL at creation time; immutable after this point.
	var target *url.URL
	if info, ok := m.stateWatcher.GetState(ctx, app.ID); ok {
		target = info.UpstreamTarget // pre-parsed by watcher on CRD event; may be nil
	}

	// Create reverse proxy
	upstream = &AppUpstream{
		manager: m,
		app:     app,
		target:  target,
	}
	upstream.handler = upstream.newProxy(m.config.Upstream.HTTPTimeout)
	upstream.wsHandler = upstream.newWebsocketProxy(m.config.Upstream.WsTimeout)

	// WebSocket activity tracking is per-frame (see newWebsocketProxy).
	// No periodic ticker is needed — idle connections (only ping/pong) must
	// not keep the app alive.

	return upstream, nil
}

func (u *AppUpstream) ServeHTTPOrError(rw http.ResponseWriter, req *http.Request) error {
	ctx := req.Context()

	// K8s state pre-check: if we know the app is not running, handle it synchronously
	// without attempting DNS/upstream. Falls through if state is unknown or Running.
	if appInfo, ok := u.manager.stateWatcher.GetState(ctx, u.app.ID); ok && appInfo.ActualState != k8sapp.AppActualStateRunning {
		switch {
		case appInfo.ActualState == k8sapp.AppActualStateStarting:
			u.manager.pageWriter.WriteSpinnerPage(rw, req, u.app)
		case appInfo.DevMode, !appInfo.AutoRestartEnabled:
			// In DEV mode (per App CRD spec.devMode.enabled) auto-resume is
			// disabled — only the owner of the dev/prod switch may bring
			// the app back to Running.
			u.manager.pageWriter.WriteRestartDisabledPage(rw, req, u.app)
		case isFrameworkBackgroundPoll(req.URL.Path):
			// Auto-suspended app + framework background poll (e.g. Streamlit's
			// /_stcore/health emitted by the frontend on its WS reconnect
			// cycle while the tab stays open). Triggering a wakeup here would
			// defeat auto-suspend on every forgotten tab, so instead we serve a
			// 503 with a plain-text message that the frontend shows in its
			// connection modal ("paused due to inactivity, refresh to start").
			// The user has to perform a meaningful action (reload) to wake the
			// app, which lands on a non-poll path (GET /) and falls into the
			// default branch below.
			u.manager.pageWriter.WriteSuspendedPage(rw)
		default:
			u.wakeup(ctx, errors.Errorf("app state is %s", appInfo.ActualState))
			u.manager.pageWriter.WriteSpinnerPage(rw, req, u.app)
		}
		return nil
	}

	// Target set at creation time; nil means appsProxy.upstreamUrl was absent.
	if u.target == nil {
		u.manager.logger.Infof(ctx, "app %q has no upstream URL, serving spinner page", u.app.ID)
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
	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(u.target)
			r.Out.URL.RawQuery = r.In.URL.RawQuery

			// Rewrite strips all X-Forwarded-* headers before calling us.
			// Restore incoming X-Forwarded-For so SetXForwarded can append
			// this hop's IP to the existing chain (it only appends to For).
			r.Out.Header["X-Forwarded-For"] = r.In.Header["X-Forwarded-For"]
			r.SetXForwarded()

			// SetXForwarded overwrites Host and Proto with values derived
			// from r.In (this hop). If an upstream proxy (e.g. a
			// TLS-terminating LB) already set them, restore those values
			// since they reflect the original client request more accurately.
			if h := r.In.Header["X-Forwarded-Host"]; len(h) > 0 {
				r.Out.Header["X-Forwarded-Host"] = h
			}
			// Some ingress controllers (e.g. Nginx Ingress on GKE) set
			// X-Forwarded-Scheme instead of X-Forwarded-Proto. Fall back to
			// it so the upstream sees the correct original scheme.
			if p := r.In.Header.Get("X-Forwarded-Proto"); p != "" {
				r.Out.Header.Set("X-Forwarded-Proto", p)
			} else if s := r.In.Header.Get("X-Forwarded-Scheme"); s != "" {
				r.Out.Header.Set("X-Forwarded-Proto", s)
			}

			// Inject E2B access token for E2B sandbox apps.
			// Always fetch the latest token from the state watcher to handle
			// secret recreation (updates propagate asynchronously).
			r.Out.Header.Del("e2b-traffic-access-token")
			if info, ok := u.manager.AppInfo(r.Out.Context(), u.app.ID); ok && info.E2BAccessToken != "" {
				r.Out.Header.Set("e2b-traffic-access-token", info.E2BAccessToken)
			}
		},
		Transport:    u.manager.transport,
		ErrorHandler: u.manager.pageWriter.ProxyErrorHandlerFor(u.app),
	}
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

	// ******************************************************************************
	// TEMPORARY WORKAROUND — remove once Streamlit apps are configured with
	// STREAMLIT_BROWSER_SERVER_ADDRESS / STREAMLIT_BROWSER_SERVER_PORT env vars.
	//
	// Streamlit (Tornado) checks WebSocket origin by comparing the Origin header
	// against the Host header. Since apps-proxy rewrites Host to the upstream
	// hostname (required for LB routing), Origin (the public domain set by the
	// browser) no longer matches and Tornado rejects the connection with 403.
	//
	// Rewriting Origin to the upstream hostname makes the request look like a
	// direct browser connection, which is what every framework expects.
	// ******************************************************************************
	if u.target != nil {
		upstreamOrigin := u.target.Scheme + "://" + u.target.Host
		origRewrite := proxy.Rewrite
		proxy.Rewrite = func(r *httputil.ProxyRequest) {
			origRewrite(r)
			r.Out.Header.Set("Origin", upstreamOrigin)
		}
	}

	// Per-frame WebSocket activity tracking.
	//
	// httputil.ReverseProxy.handleUpgradeResponse type-asserts res.Body to
	// io.ReadWriteCloser and uses it as the upstream end of a bidirectional
	// copy with the hijacked client conn. Wrapping res.Body once therefore
	// observes both directions of the WebSocket stream:
	//
	//   - Read on the wrapped RWC = server→client bytes (unmasked frames),
	//   - Write on the wrapped RWC = client→server bytes (masked frames).
	//
	// wsactivity.Wrap parses frame headers in both directions and invokes
	// the callback once per non-control frame. notify.Manager already
	// throttles per app to one outbound call per 30s, so we can fire the
	// callback per frame without flooding the Sandboxes Service.
	proxy.ModifyResponse = func(res *http.Response) error {
		if res.StatusCode != http.StatusSwitchingProtocols {
			return nil
		}
		rwc, ok := res.Body.(io.ReadWriteCloser)
		if !ok {
			// httputil.ReverseProxy itself requires this assertion to succeed
			// for a 101 response; if it doesn't, the proxy errors out anyway.
			// We just skip wrapping and let the proxy report the error.
			return nil
		}
		// Bind the callback to the request context so notify() can decorate
		// its span/log with the right request attributes. notify() itself
		// uses context.WithoutCancel, so the in-flight call survives the WS
		// timeout and any per-request cancellation.
		reqCtx := res.Request.Context()
		res.Body = wsactivity.Wrap(rwc, func() { u.notify(reqCtx) })
		return nil
	}

	return chain.
		New(chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
			ctx := ctxattr.ContextWith(req.Context(), attribute.Bool(attrWebsocket, true))
			ctx, cancel := context.WithTimeoutCause(ctx, timeout, errors.New("upstream websocket request timeout"))
			defer cancel()

			ctx, c := context.WithCancelCause(ctx)
			u.cancelWs = c

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
			// Capture the path at request scope — it must be available inside
			// the GotConn callback closure below, which only receives a
			// connInfo argument.
			reqPath := req.URL.Path

			// Trace connection events. Background polls emitted by data-app
			// frontends independent of user interaction (see
			// isFrameworkBackgroundPoll) are not considered activity and do
			// not bump lastRequestTimestamp.
			reqCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				GotConn: func(connInfo httptrace.GotConnInfo) {
					if isFrameworkBackgroundPoll(reqPath) {
						return
					}
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

// isFrameworkBackgroundPoll reports whether the given URL path is a known
// data-app frontend background-poll endpoint that fires independently of user
// interaction.
//
// Currently covers Streamlit's /_stcore/health and /_stcore/host-config. These
// are emitted on every WebSocket (re)connect — including the periodic ~20 min
// reconnect cycle imposed by an external idle timeout — and would otherwise
// either bump lastRequestTimestamp on a Running app (defeating auto-suspend)
// or wake a Suspended one (defeating it again). Apps-proxy treats them as
// non-activity: notify is skipped on a Running app and the request is rejected
// with 503 Retry-After on a Suspended one, requiring the user to perform a
// meaningful action (refresh, click into the UI) to wake the app.
func isFrameworkBackgroundPoll(path string) bool {
	switch path {
	case "/_stcore/health", "/_stcore/host-config":
		return true
	default:
		return false
	}
}
