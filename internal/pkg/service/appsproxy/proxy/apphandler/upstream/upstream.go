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
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/wakeup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	notifyRequestTimeout = 5 * time.Second
	wakeupRequestTimeout = 5 * time.Second
	attrWakeupReason     = "proxy.wakeup.reason"
	attrWebsocket        = "proxy.websocket"
)

type Manager struct {
	wg           *sync.WaitGroup
	logger       log.Logger
	telemetry    telemetry.Telemetry
	transport    http.RoundTripper
	pageWriter   *pagewriter.Writer
	configLoader *appconfig.Loader
	notify       *notify.Manager
	wakeup       *wakeup.Manager
}

type AppUpstream struct {
	manager   *Manager
	app       api.AppConfig
	target    *url.URL
	handler   *chain.Chain
	wsHandler *chain.Chain
}

type dependencies interface {
	Process() *servicectx.Process
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	UpstreamTransport() http.RoundTripper
	PageWriter() *pagewriter.Writer
	AppConfigLoader() *appconfig.Loader
	NotifyManager() *notify.Manager
	WakeupManager() *wakeup.Manager
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

func (m *Manager) NewUpstream(ctx context.Context, app api.AppConfig) (upstream *AppUpstream, err error) {
	ctx, span := m.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.upstream.NewUpstream")
	defer span.End(&err)

	// Parse target
	target, err := url.Parse(app.UpstreamAppURL)
	if err != nil {
		return nil, svcErrors.NewServiceUnavailableError(errors.PrefixErrorf(err,
			`unable to parse upstream url for app "%s" "%s"`, app.ID, app.Name,
		))
	}

	// Create reverse proxy
	upstream = &AppUpstream{manager: m, app: app, target: target}
	upstream.handler = upstream.newProxy()
	upstream.wsHandler = upstream.newWebsocketProxy()
	return upstream, nil
}

func (u *AppUpstream) ServeHTTPOrError(rw http.ResponseWriter, req *http.Request) error {
	// Difference between regular and websocket request
	if strings.EqualFold(req.Header.Get("Connection"), "upgrade") && req.Header.Get("Upgrade") == "websocket" {
		return u.wsHandler.ServeHTTPOrError(rw, req)
	} else {
		return u.handler.ServeHTTPOrError(rw, req)
	}
}

func (u *AppUpstream) newProxy() *chain.Chain {
	proxy := httputil.NewSingleHostReverseProxy(u.target)
	proxy.Transport = u.manager.transport
	proxy.ErrorHandler = u.manager.pageWriter.ProxyErrorHandler

	return chain.
		New(chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
			ctx := ctxattr.ContextWith(req.Context(), attribute.Bool(attrWebsocket, false))
			proxy.ServeHTTP(w, req.WithContext(ctx))
			return nil
		})).
		Prepend(
			// Trace connection events
			u.trace(),
		)
}

func (u *AppUpstream) newWebsocketProxy() *chain.Chain {
	proxy := httputil.NewSingleHostReverseProxy(u.target)
	proxy.Transport = u.manager.transport
	proxy.ErrorHandler = u.manager.pageWriter.ProxyErrorHandler

	return chain.
		New(chain.HandlerFunc(func(w http.ResponseWriter, req *http.Request) error {
			ctx := ctxattr.ContextWith(req.Context(), attribute.Bool(attrWebsocket, true))
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
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				GotConn: func(connInfo httptrace.GotConnInfo) {
					u.notify(ctx)
				},
				DNSDone: func(info httptrace.DNSDoneInfo) {
					if info.Err != nil {
						u.wakeup(ctx, info.Err)
					}
				},
			})

			return next.ServeHTTPOrError(w, req.WithContext(ctx))
		})
	}
}

func (u *AppUpstream) notify(ctx context.Context) {
	// The request should not wait for the notification
	u.manager.wg.Add(1)
	go func() {
		defer u.manager.wg.Done()

		notificationCtx, cancel := context.WithTimeout(context.Background(), notifyRequestTimeout)
		defer cancel()

		notificationCtx = ctxattr.ContextWith(notificationCtx, ctxattr.Attributes(ctx).ToSlice()...)

		_, span := u.manager.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.upstream.notify")
		notificationCtx = telemetry.ContextWithSpan(notificationCtx, span)

		// Error is already logged by the Notify method itself.
		err := u.manager.notify.Notify(notificationCtx, u.app.ID) //nolint:contextcheck
		span.End(&err)
	}()
}

func (u *AppUpstream) wakeup(ctx context.Context, err error) {
	// The request should not wait for the wakeup request
	u.manager.wg.Add(1)
	go func() {
		defer u.manager.wg.Done()

		wakeupCtx, cancel := context.WithTimeout(context.Background(), wakeupRequestTimeout)
		defer cancel()

		wakeupCtx = ctxattr.ContextWith(wakeupCtx, ctxattr.Attributes(ctx).ToSlice()...)

		_, span := u.manager.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.upstream.wakeup")
		span.SetAttributes(attribute.String(attrWakeupReason, err.Error()))
		wakeupCtx = telemetry.ContextWithSpan(wakeupCtx, span)

		// Error is already logged by the Wakeup method itself.
		err := u.manager.wakeup.Wakeup(wakeupCtx, u.app.ID) //nolint:contextcheck
		span.End(&err)
	}()
}
