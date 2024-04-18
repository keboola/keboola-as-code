package http

import (
	"context"
	"embed"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/justinas/alice"
	"go.opentelemetry.io/otel/attribute"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	logger            log.Logger
	telemetry         telemetry.Telemetry
	config            config.Config
	clock             clock.Clock
	loader            *appconfig.Loader
	selectionTemplate *template.Template
	exceptionIDPrefix string
	wg                sync.WaitGroup
}

const providerCookie = "_oauth2_provider"

const selectionPagePath = "/_proxy/selection"

//go:embed template/*
var templates embed.FS

func NewRouter(d dependencies.ServiceScope, exceptionIDPrefix string) (*Router, error) {
	html, err := templates.ReadFile("template/selection.html.tmpl")
	if err != nil {
		return nil, errors.PrefixError(err, "selection template file not found")
	}

	tmpl, err := template.New("selection template").Parse(string(html))
	if err != nil {
		return nil, errors.PrefixError(err, "could not parse selection template")
	}

	router := &Router{
		logger:            d.Logger(),
		telemetry:         d.Telemetry(),
		config:            d.Config(),
		clock:             d.Clock(),
		loader:            d.AppConfigLoader(),
		selectionTemplate: tmpl,
		exceptionIDPrefix: exceptionIDPrefix,
		wg:                sync.WaitGroup{},
	}

	return router, nil
}

func (r *Router) CreateHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()

		appID, ok := ctx.Value(AppIDCtxKey).(string)
		if !ok {
			if req.URL.Path == "/health-check" {
				w.WriteHeader(http.StatusOK)
				return
			}

			r.logger.Info(req.Context(), `unable to find application ID from the URL`)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `Unable to find application ID from the URL.`)
			return
		}

		// Delete all X-KBC-* headers as they're used for internal information.
		for name := range req.Header {
			if strings.HasPrefix(strings.ToLower(name), "x-kbc-") {
				req.Header.Del(name)
			}
		}

		// Load configuration for given app.
		cfg, modified, err := r.loader.GetConfig(req.Context(), api.AppID(appID))
		if err != nil {
			var apiErr *api.Error
			errors.As(err, &apiErr)
			if apiErr != nil && apiErr.StatusCode() == http.StatusNotFound {
				r.logger.Infof(req.Context(), `application "%s" not found`, appID)
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, `Application "%s" not found.`, appID)
				return
			}

			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `Unable to load configuration for app "%s":%s.`, appID, err)
			return
		}

		// Recreate app handler if configuration changed.
		handler := r.appHandlers.GetOrInit(appID)
		httpHandler := handler.getHTTPHandler()

		if modified || httpHandler == nil {
			handler.setHTTPHandler(func() http.Handler {
				httpHandler = r.createDataAppHandler(req.Context(), cfg)
				return httpHandler
			})
		}

		httpHandler.ServeHTTP(w, req)
	})
}

func (r *Router) Shutdown() {
	r.wg.Wait()
}

func (r *Router) createConfigErrorHandler(exceptionID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warn(req.Context(), `application "<proxy.appid>" has misconfigured OAuth2 provider`)
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintln(w, "Application has invalid configuration.")
		fmt.Fprintln(w, "Exception ID:", exceptionID)
	})
}

func (r *Router) createDataAppHandler(ctx context.Context, app api.AppConfig) http.Handler {
	if len(app.AuthRules) == 0 {
		exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `no rules defined for app "<proxy.appid>" "%s"`, app.Name)
		return r.createConfigErrorHandler(exceptionID)
	}

	target, err := url.Parse(app.UpstreamAppURL)
	if err != nil {
		exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unable to parse upstream url for app "<proxy.appid>" "%s"`, app.Name)
		return r.createConfigErrorHandler(exceptionID)
	}

	chain := alice.New(r.notifySandboxesServiceMiddleware(), r.dataAppTelemetryMiddleware())

	oauthProviders := make(map[provider.ID]*oauthProvider)
	for _, providerConfig := range app.AuthProviders {
		oauthProviders[providerConfig.ID()] = r.createProvider(ctx, providerConfig, app, chain)
	}

	publicAppHandler := r.publicAppHandler(target, chain)

	mux := http.NewServeMux()

	mux.Handle(selectionPagePath, r.createSelectionPageHandler(oauthProviders))

	// Always send /_proxy/ requests to the correct provider.
	// This is necessary for proxy callback url to work on an app with prefixed private parts.
	mux.Handle("/_proxy/", r.createMultiProviderHandler(oauthProviders, app))

	for _, rule := range app.AuthRules {
		err := rule.RegisterHandler(mux, r.createRuleHandler(ctx, app, rule, publicAppHandler, oauthProviders))
		if err != nil {
			exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
			r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `invalid rule "%s" "%s": %s`, app.ID.String(), rule.Type, err)
			return r.createConfigErrorHandler(exceptionID)
		}
	}

	return mux
}

func (r *Router) createRuleHandler(ctx context.Context, app api.AppConfig, rule api.Rule, publicAppHandler http.Handler, oauthProviders map[provider.ID]*oauthProvider) http.Handler {
	// If AuthRequired is unset, use true by default
	authRequired := true
	if rule.AuthRequired != nil {
		authRequired = *rule.AuthRequired
	}

	if !authRequired {
		if len(rule.Auth) > 0 {
			exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
			r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unexpected auth while authRequired is false for app "<proxy.appid>" "%s"`, app.Name)
			return r.createConfigErrorHandler(exceptionID)
		}

		return publicAppHandler
	}

	if len(rule.Auth) == 0 {
		exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `empty providers array for app "<proxy.appid>" "%s"`, app.Name)
		return r.createConfigErrorHandler(exceptionID)
	}

	selectedProviders := make(map[provider.ID]*oauthProvider)
	for _, id := range rule.Auth {
		authProvider, found := oauthProviders[id]
		if !found {
			exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
			r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unexpected provider id "%s" for app "<proxy.appid>" "%s"`, id, app.Name)
			return r.createConfigErrorHandler(exceptionID)
		}

		selectedProviders[id] = authProvider
	}

	return r.createMultiProviderHandler(selectedProviders, app)
}

func (r *Router) dataAppTelemetryMiddleware() alice.Constructor {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			ctx, span := r.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.app.request")
			defer span.End(nil)
			req = req.WithContext(ctx)
			next.ServeHTTP(w, req)
		})
	}
}

func (r *Router) formatAppDomain(app api.AppConfig) string {
	domain := app.ID.String() + "." + r.config.API.PublicURL.Host
	if app.Name != "" {
		domain = app.Name + "-" + domain
	}
	return domain
}
