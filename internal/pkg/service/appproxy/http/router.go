package http

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/binary"
	"fmt"
	"html/template"
	"io"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/cookies"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	logger            log.Logger
	telemetry         telemetry.Telemetry
	config            config.Config
	clock             clock.Clock
	handlers          map[AppID]http.Handler
	selectionTemplate *template.Template
	exceptionIDPrefix string
}

const ProviderCookie = "_oauth2_provider"

const selectionPagePath = "/_proxy/selection"

//go:embed template/*
var templates embed.FS

func NewRouter(ctx context.Context, d dependencies.ServiceScope, exceptionIDPrefix string, apps []DataApp) (*Router, error) {
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
		handlers:          map[AppID]http.Handler{},
		selectionTemplate: tmpl,
		exceptionIDPrefix: exceptionIDPrefix,
	}

	for _, app := range apps {
		router.handlers[app.ID] = router.createDataAppHandler(ctx, app)
	}

	return router, nil
}

func (r *Router) CreateHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		appIDString, ok := ctxattr.Attributes(req.Context()).Value(attrAppID)
		if !ok {
			if req.URL.Path == "/health-check" {
				w.WriteHeader(http.StatusOK)
				return
			}

			r.logger.Info(req.Context(), `unable to parse application ID from the URL`)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `Unable to parse application ID from the URL.`)
			return
		}

		// Delete all X-Kbc-* headers as they're used for internal information.
		for name := range req.Header {
			if strings.HasPrefix(name, "X-Kbc-") {
				req.Header.Del(name)
			}
		}

		appID := AppID(appIDString.Emit())

		if handler, found := r.handlers[appID]; found {
			handler.ServeHTTP(w, req)
		} else {
			r.logger.Infof(req.Context(), `application "%s" not found`, appID)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `Application "%s" not found.`, appID)
		}
	})
}

func (r *Router) createConfigErrorHandler(exceptionID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.logger.Warn(req.Context(), `application has misconfigured OAuth2 provider`)
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintln(w, "Application has misconfigured OAuth2 provider.")
		fmt.Fprintln(w, "Exception ID: ", exceptionID)
	})
}

func (r *Router) createDataAppHandler(ctx context.Context, app DataApp) http.Handler {
	if len(app.Rules) == 0 {
		exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
		r.logger.With("exceptionId", exceptionID).Warnf(ctx, `no rules defined for app "<proxy.appid>" "%s"`, app.Name)
		return r.createConfigErrorHandler(exceptionID)
	}

	oauthProviders := make(map[string]*oauthProvider)
	for _, providerConfig := range app.Providers {
		oauthProviders[providerConfig.ID] = r.createProvider(ctx, providerConfig, app)
	}

	publicAppHandler := r.publicAppHandler(app)

	mux := http.NewServeMux()

	mux.Handle(selectionPagePath, r.createSelectionPageHandler(oauthProviders))

	// Always send /_proxy/ requests to the correct provider.
	// This is necessary for proxy callback url to work on an app with prefixed private parts.
	mux.Handle("/_proxy/", r.createMultiProviderHandler(oauthProviders))

	for _, rule := range app.Rules {
		if rule.Type != PathPrefix {
			exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
			r.logger.With("exceptionId", exceptionID).Warnf(ctx, `unexpected rule type "%s" for app "<proxy.appid>" "%s"`, rule.Type, app.Name)
			return r.createConfigErrorHandler(exceptionID)
		}

		mux.Handle(rule.Value, r.createRuleHandler(ctx, app, publicAppHandler, oauthProviders, rule.Providers))
	}

	return mux
}

func (r *Router) createRuleHandler(ctx context.Context, app DataApp, publicAppHandler http.Handler, oauthProviders map[string]*oauthProvider, providers []string) http.Handler {
	if len(providers) == 0 {
		return publicAppHandler
	}

	selectedProviders := make(map[string]*oauthProvider)
	for _, id := range providers {
		provider, found := oauthProviders[id]
		if !found {
			exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
			r.logger.With("exceptionId", exceptionID).Warnf(ctx, `unexpected provider id "%s" for app "<proxy.appid>" "%s"`, id, app.Name)
			return r.createConfigErrorHandler(exceptionID)
		}

		selectedProviders[id] = provider
	}

	return r.createMultiProviderHandler(selectedProviders)
}

func (r *Router) publicAppHandler(app DataApp) http.Handler {
	target := &url.URL{
		Scheme: "http",
		Host:   app.UpstreamHost,
	}

	return httputil.NewSingleHostReverseProxy(target)
}

type oauthProvider struct {
	providerConfig options.Provider
	proxyConfig    *options.Options
	proxyProvider  providers.Provider
	handler        http.Handler
}

func (r *Router) createProvider(ctx context.Context, providerConfig options.Provider, app DataApp) *oauthProvider {
	authValidator := func(email string) bool {
		// No need to verify users, just groups which is done using AllowedGroups in provider configuration.
		return true
	}

	exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()

	provider := &oauthProvider{
		providerConfig: providerConfig,
		handler:        r.createConfigErrorHandler(exceptionID),
	}

	proxyConfig, err := r.authProxyConfig(app, providerConfig)
	if err != nil {
		r.logger.With("exceptionId", exceptionID).Warnf(ctx, `unable to create oauth proxy config for app "%s" "%s": %s`, app.ID, app.Name, err.Error())
		return provider
	}
	provider.proxyConfig = proxyConfig

	proxyProvider, err := providers.NewProvider(providerConfig)
	if err != nil {
		r.logger.With("exceptionId", exceptionID).Warnf(ctx, `unable to create oauth provider for app "%s" "%s": %s`, app.ID, app.Name, err.Error())
		return provider
	}
	provider.proxyProvider = proxyProvider

	proxy, err := oauthproxy.NewOAuthProxy(proxyConfig, authValidator)
	if err != nil {
		r.logger.With("exceptionId", exceptionID).Warnf(ctx, `unable to start oauth proxy for app "%s" "%s": %s`, app.ID, app.Name, err.Error())
		return provider
	}
	provider.handler = proxy

	return provider
}

type SelectionPageData struct {
	Providers []SelectionPageProvider
}

type SelectionPageProvider struct {
	Name string
	URL  string
}

func (r *Router) createSelectionPageHandler(oauthProviders map[string]*oauthProvider) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		selection := request.URL.Query().Get("provider")
		provider, ok := oauthProviders[selection]

		if !ok {
			// Render selection page
			data := SelectionPageData{}
			for id, oauthProvider := range oauthProviders {
				providerURL := &url.URL{
					Scheme:   r.config.PublicAddress.Scheme,
					Host:     request.Host,
					Path:     selectionPagePath,
					RawQuery: "provider=" + url.PathEscape(id),
				}

				data.Providers = append(data.Providers, SelectionPageProvider{
					Name: oauthProvider.providerConfig.Name,
					URL:  providerURL.String(),
				})
			}

			writer.WriteHeader(http.StatusForbidden)
			err := r.selectionTemplate.Execute(writer, data)
			if err != nil {
				r.logger.Error(request.Context(), "could not execute template")
			}

			return
		}

		if provider.proxyConfig != nil {
			// Set cookie value to the selected provider
			http.SetCookie(writer, cookies.MakeCookieFromOptions(
				request,
				ProviderCookie,
				selection,
				&provider.proxyConfig.Cookie,
				provider.proxyConfig.Cookie.Expire,
				r.clock.Now(),
			))
		}

		// Request using the selected provider to trigger a redirect to the provider's sign in page
		newRequest, err := http.NewRequestWithContext(request.Context(), http.MethodGet, "/", nil)
		if err != nil {
			r.logger.Error(request.Context(), "could not create request")
		}

		provider.handler.ServeHTTP(writer, newRequest)
	})
}

// OAuth2 Proxy doesn't support multiple providers despite the possibility of setting them up in configuration.
// So instead we're using separate proxy instance for each provider with a cookie to remember the selection.
// See https://github.com/oauth2-proxy/oauth2-proxy/issues/926
func (r *Router) createMultiProviderHandler(oauthProviders map[string]*oauthProvider) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var provider *oauthProvider
		ok := false

		// Identify the provider chosen by the user using a cookie
		cookie, err := request.Cookie(ProviderCookie)
		if err == nil {
			provider, ok = oauthProviders[cookie.Value]
		}

		if !ok {
			if len(oauthProviders) == 1 {
				// If only one provider is available for current path prefix, immediately set the cookie to it.
				// It is necessary because even if this prefix doesn't have multiple providers, other prefix might.
				// The /_proxy/callback url would not work correctly without the cookie.

				// Use maps.Keys() instead when possible: https://github.com/golang/go/issues/61900
				var k string
				for k = range oauthProviders {
				}
				provider := oauthProviders[k]

				if provider.proxyConfig != nil {
					http.SetCookie(writer, cookies.MakeCookieFromOptions(
						request,
						ProviderCookie,
						provider.providerConfig.ID,
						&provider.proxyConfig.Cookie,
						provider.proxyConfig.Cookie.Expire,
						r.clock.Now(),
					))
				}

				provider.handler.ServeHTTP(writer, request)
			} else {
				// Clear the provider cookie in case it existed with an invalid value
				http.SetCookie(writer, cookies.MakeCookieFromOptions(
					request,
					ProviderCookie,
					"",
					&options.NewOptions().Cookie,
					time.Hour*-1,
					r.clock.Now(),
				))

				r.redirectToProviderSelection(writer, request)
			}

			return
		}

		if provider.proxyProvider != nil {
			loginURL := provider.proxyProvider.Data().LoginURL

			// If oauthproxy returns a redirect to login page, we instead redirect to provider selection page
			writer = NewCallbackResponseWriter(writer, func(writer http.ResponseWriter, statusCode int) {
				if statusCode != http.StatusFound {
					return
				}

				locationURL, err := url.Parse(writer.Header().Get("Location"))
				if err != nil {
					return
				}

				// Redirect to OAuth2 provider is instead redirected to selection page
				if locationURL.Host == loginURL.Host && locationURL.Path == loginURL.Path {
					r.redirectToProviderSelection(writer, request)
				}
			})
		}

		// Authenticate the request by the provider selected in the cookie
		provider.handler.ServeHTTP(writer, request)
	})
}

func (r *Router) redirectToProviderSelection(writer http.ResponseWriter, request *http.Request) {
	selectionPageURL := &url.URL{
		Scheme: r.config.PublicAddress.Scheme,
		Host:   request.Host,
		Path:   selectionPagePath,
	}

	writer.Header().Set("Location", selectionPageURL.String())
	writer.WriteHeader(http.StatusFound)
}

func (r *Router) authProxyConfig(app DataApp, provider options.Provider) (*options.Options, error) {
	v := options.NewOptions()

	domain := app.ID.String() + "." + r.config.PublicAddress.Host

	secret, err := r.generateCookieSecret(app, provider)
	if err != nil {
		return v, err
	}

	v.Cookie.Secret = string(secret)
	v.Cookie.Domains = []string{domain}
	v.Cookie.SameSite = "strict"
	v.ProxyPrefix = "/_proxy"
	v.RawRedirectURL = r.config.PublicAddress.Scheme + "://" + domain + v.ProxyPrefix + "/callback"

	v.Providers = options.Providers{provider}
	v.SkipProviderButton = true
	v.Session = options.SessionOptions{Type: options.CookieSessionStoreType}
	v.EmailDomains = []string{"*"}
	v.InjectRequestHeaders = []options.Header{
		headerFromClaim("X-Kbc-User-Name", "name"),
		headerFromClaim("X-Kbc-User-Email", options.OIDCEmailClaim),
		headerFromClaim("X-Kbc-User-Roles", options.OIDCGroupsClaim),
	}
	v.UpstreamServers = options.UpstreamConfig{
		Upstreams: []options.Upstream{
			{
				ID:   app.ID.String(),
				Path: "/",
				URI:  "http://" + app.UpstreamHost,
			},
		},
	}

	// Cannot separate errors from info because when ErrToInfo is false (default),
	// oauthproxy keeps forcibly setting its global error writer to os.Stderr whenever a new proxy instance is created.
	v.Logging.ErrToInfo = true

	if err := validation.Validate(v); err != nil {
		return nil, err
	}

	return v, nil
}

// generateCookieSecret creates a unique cookie secret for each app and provider.
// This is necessary because otherwise cookies created by provider A would also be valid in a section that requires provider B but not A.
// To solve this we use the combination of the provider id and our cookie secret as a seed for the real cookie secret.
// App ID is also used as part of the seed because cookies for app X cannot be valid for app Y even if they're using the same provider.
func (r *Router) generateCookieSecret(app DataApp, provider options.Provider) ([]byte, error) {
	if r.config.CookieSecretSalt == "" {
		return nil, errors.New("missing cookie secret salt")
	}

	h := sha256.New()
	if _, err := io.WriteString(h, app.ID.String()+"/"+provider.ID+"/"+r.config.CookieSecretSalt); err != nil {
		return nil, err
	}
	seed := binary.BigEndian.Uint64(h.Sum(nil))

	secret := make([]byte, 32)
	random := rand.New(rand.NewSource(int64(seed))) // nolint: gosec // crypto.rand doesn't accept a seed
	if _, err := random.Read(secret); err != nil {
		return nil, err
	}
	return secret, nil
}

func headerFromClaim(header, claim string) options.Header {
	return options.Header{
		Name: header,
		Values: []options.HeaderValue{
			{
				ClaimSource: &options.ClaimSource{
					Claim: claim,
				},
			},
		},
	}
}
