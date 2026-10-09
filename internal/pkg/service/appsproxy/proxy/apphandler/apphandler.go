// Package apphandler provides http.Handler for a data app.
// It connects: upstream without authentication, authentication handlers and path matching.
package apphandler

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type appHandler struct {
	manager            *Manager
	app                api.AppConfig
	workload           k8sapp.WorkloadRef
	baseURL            *url.URL
	attrs              []attribute.KeyValue
	upstream           chain.Handler
	allAuthHandlers    chain.Handler
	authHandlerPerRule map[ruleIndex]chain.Handler
}

type ruleIndex int

func newAppHandler(manager *Manager, app api.AppConfig, workload k8sapp.WorkloadRef, appUpstream chain.Handler, authHandlers map[provider.ID]selector.Handler) (http.Handler, error) {
	handler := &appHandler{
		manager:            manager,
		app:                app,
		workload:           workload,
		baseURL:            app.BaseURL(manager.config.API.PublicURL),
		attrs:              app.Telemetry(),
		upstream:           appUpstream,
		authHandlerPerRule: make(map[ruleIndex]chain.Handler),
	}

	// Create handler with all auth handlers, to route internal URLs
	if len(authHandlers) > 0 {
		if h, err := manager.authProxyManager.ProviderSelector().For(app, authHandlers); err == nil {
			handler.allAuthHandlers = h
		} else {
			return nil, err
		}
	}

	// There must be at lest one auth rule
	if len(app.AuthRules) == 0 {
		return nil, errors.New(`no path rule is configured`)
	}

	// Prepare authentication handlers for each rule
	for i, rule := range app.AuthRules {
		index := ruleIndex(i)

		// Auth is required by default
		if rule.AuthRequired != nil && !*rule.AuthRequired {
			// There must be no auth provider, if the auth is not required
			if len(rule.Auth) > 0 {
				return nil, errors.Errorf(`no authentication provider is expected for "%s"`, rule.Value)
			}

			// No authentication
			continue
		}

		// There must be at least one auth provider, if the auth is required
		if len(rule.Auth) == 0 {
			return nil, errors.Errorf(`no authentication provider is configured for "%s"`, rule.Value)
		}

		// Filter authentication handlers
		authHandlersPerRule := make(map[provider.ID]selector.Handler)
		for _, providerID := range rule.Auth {
			if authHandler, found := authHandlers[providerID]; found {
				authHandlersPerRule[providerID] = authHandler
			} else {
				return nil, errors.Errorf(`authentication provider "%s" not found for "%s"`, providerID.String(), rule.Value)
			}
		}

		// Merge authentication handlers for the rule to one selector handler
		if h, err := manager.authProxyManager.ProviderSelector().For(app, authHandlersPerRule); err == nil {
			handler.authHandlerPerRule[index] = h
		} else {
			return nil, err
		}
	}

	return handler, nil
}

func (h *appHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if err := h.serveHTTPOrError(w, req); err != nil {
		h.manager.pageWriter.WriteError(w, req, &h.app, err)
	}
}

// serveHTTPOrError delegates error handling to the upper method.
func (h *appHandler) serveHTTPOrError(w http.ResponseWriter, req *http.Request) (err error) {
	ctx := req.Context()

	// Enrich root span with attributes
	reqSpan, ok := middleware.RequestSpan(ctx)
	if ok {
		reqSpan.SetAttributes(h.attrs...)
	}

	// Enrich context with attributes
	ctx = ctxattr.ContextWith(ctx, h.attrs...)

	// Enrich telemetry
	labeler, _ := otelhttp.LabelerFromContext(ctx)
	labeler.Add(h.attrs...)

	// Trace the request
	ctx, span := h.manager.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.app.request")
	defer span.End(&err)

	req = req.WithContext(ctx)

	// Delete all X-KBC-* headers as they're used for internal information.
	for name := range req.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-kbc-") {
			req.Header.Del(name)
		}
	}

	// Add request ID header, for OAuth2Proxy internals and app itself
	if id := middleware.RequestIDFromContext(req.Context()); id != "" { //nolint:contextcheck // false positive
		req.Header.Set(config.RequestIDHeader, id)
	}

	// Redirect request to canonical host to match cookies domain.
	//
	// Only the App needs this. parseAppID strips the subdomain to the last dash,
	// so an App answers on many hostnames and a cookie set on one would not be
	// sent to another. A Sandbox owns exactly one hostname and is reached only by
	// matching it exactly, before parseAppID is consulted, so it has nothing to
	// canonicalise and needs no canonical host of its own.
	if !h.workload.IsSandbox() && strings.ToLower(req.Host) != h.baseURL.Host {
		w.Header().Set("Location", h.baseURL.ResolveReference(&url.URL{Path: req.URL.Path, RawQuery: req.URL.RawQuery}).String())
		w.WriteHeader(http.StatusPermanentRedirect)
		return nil
	}

	if req.URL.Path == preview.Path {
		return h.servePreviewEndpoint(w, req)
	}

	// Removed from every request, so no upstream path ever sees it.
	previewCookie := session.TakeCookie(req)

	if req.URL.Path == selector.SignOutPath {
		h.clearPreviewSession(w, previewCookie)
		previewCookie = ""
	}

	// Internal paths are routed before any preview session, so sign-out and the OIDC callback never reach the app.
	if strings.HasPrefix(req.URL.Path, config.InternalPrefix) && h.allAuthHandlers != nil {
		// A sign-out ends the session explicitly. Internal paths never reach
		// the upstream, so this cannot be handled by the sessions middleware.
		if req.URL.Path == selector.SignOutPath {
			h.manager.sessionsManager.SignOut(w, req, h.app)
		}
		return h.allAuthHandlers.ServeHTTPOrError(w, req)
	}

	if h.previewSessionValid(w, req, previewCookie) {
		return h.upstream.ServeHTTPOrError(w, req)
	}

	if h.previewSessionRequired(req) {
		h.serveSessionRequiredPage(w, req)
		return nil
	}

	// Find the matching rule
	for i, rule := range h.app.AuthRules {
		if ok, err := rule.Match(req); err != nil {
			return err
		} else if ok {
			return h.serveRule(w, req, ruleIndex(i))
		}
	}

	// No route found
	return svcErrors.NewResourceNotFoundError("route for", req.URL.Path, "application")
}

func (h *appHandler) serveRule(w http.ResponseWriter, req *http.Request, index ruleIndex) error {
	// Use auth handler if the request requires authentication
	if authHandler := h.authHandlerPerRule[index]; authHandler != nil {
		return authHandler.ServeHTTPOrError(w, req)
	}

	// Serve the request without authentication
	return h.upstream.ServeHTTPOrError(w, req)
}

// isDevMode reports whether this app currently has DevMode enabled.
// It reads from the live K8s state cache so toggling DevMode on the App CRD takes
// effect on the next request without requiring handler recreation.
func (h *appHandler) isDevMode(ctx context.Context) bool {
	info, ok := h.manager.upstreamManager.AppInfo(ctx, h.workload)
	return ok && info.DevMode
}
