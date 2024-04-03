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
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/justinas/alice"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/cookies"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	logger            log.Logger
	telemetry         telemetry.Telemetry
	config            config.Config
	clock             clock.Clock
	loader            dataapps.Client
	transport         *http.Transport
	appHandlers       *syncmap.SyncMap[string, appHandler]
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

	transport, err := NewReverseProxyHTTPTransport()
	if err != nil {
		return nil, errors.PrefixError(err, "could not create http transport")
	}

	router := &Router{
		logger:    d.Logger(),
		telemetry: d.Telemetry(),
		config:    d.Config(),
		clock:     d.Clock(),
		loader:    d.Loader(),
		transport: transport,
		appHandlers: syncmap.New[string, appHandler](func() *appHandler {
			return &appHandler{
				updateLock: &sync.RWMutex{},
			}
		}),
		selectionTemplate: tmpl,
		exceptionIDPrefix: exceptionIDPrefix,
		wg:                sync.WaitGroup{},
	}

	return router, nil
}

type appHandler struct {
	httpHandler http.Handler
	updateLock  *sync.RWMutex
}

func (h *appHandler) getHTTPHandler() http.Handler {
	h.updateLock.RLock()
	defer h.updateLock.RUnlock()
	return h.httpHandler
}

func (h *appHandler) setHTTPHandler(handlerFactory func() http.Handler) {
	h.updateLock.Lock()
	defer h.updateLock.Unlock()
	h.httpHandler = handlerFactory()
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
		config, modified, err := r.loader.LoadConfig(req.Context(), appID)
		if err != nil {
			var sandboxesError *dataapps.SandboxesError
			errors.As(err, &sandboxesError)
			if sandboxesError != nil && sandboxesError.StatusCode() == http.StatusNotFound {
				r.logger.Infof(req.Context(), `application "%s" not found`, appID)
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, `Application "%s" not found.`, appID)
				return
			}

			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `Unable to load configuration for app "%s".`, appID)
			return
		}

		// Recreate app handler if configuration changed.
		handler := r.appHandlers.GetOrInit(appID)
		httpHandler := handler.getHTTPHandler()

		if modified || httpHandler == nil {
			handler.setHTTPHandler(func() http.Handler {
				httpHandler = r.createDataAppHandler(req.Context(), config)
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

func (r *Router) createDataAppHandler(ctx context.Context, app dataapps.AppProxyConfig) http.Handler {
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

	oauthProviders := make(map[string]*oauthProvider)
	for _, providerConfig := range app.AuthProviders {
		oauthProviders[providerConfig.ID] = r.createProvider(ctx, providerConfig, app, chain)
	}

	publicAppHandler := r.publicAppHandler(target, chain)

	mux := http.NewServeMux()

	mux.Handle(selectionPagePath, r.createSelectionPageHandler(oauthProviders))

	// Always send /_proxy/ requests to the correct provider.
	// This is necessary for proxy callback url to work on an app with prefixed private parts.
	mux.Handle("/_proxy/", r.createMultiProviderHandler(oauthProviders, app))

	for _, rule := range app.AuthRules {
		mux.Handle(rule.Value, r.createRuleHandler(ctx, app, rule, publicAppHandler, oauthProviders))
	}

	return mux
}

func (r *Router) createRuleHandler(ctx context.Context, app dataapps.AppProxyConfig, rule dataapps.AuthRule, publicAppHandler http.Handler, oauthProviders map[string]*oauthProvider) http.Handler {
	// If AuthRequired is unset, use true by default
	authRequired := true
	if rule.AuthRequired != nil {
		authRequired = *rule.AuthRequired
	}

	if rule.Type != dataapps.PathPrefix {
		exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unexpected rule type "%s" for app "<proxy.appid>" "%s"`, rule.Type, app.Name)
		return r.createConfigErrorHandler(exceptionID)
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

	selectedProviders := make(map[string]*oauthProvider)
	for _, id := range rule.Auth {
		provider, found := oauthProviders[id]
		if !found {
			exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()
			r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unexpected provider id "%s" for app "<proxy.appid>" "%s"`, id, app.Name)
			return r.createConfigErrorHandler(exceptionID)
		}

		selectedProviders[id] = provider
	}

	return r.createMultiProviderHandler(selectedProviders, app)
}

func (r *Router) publicAppHandler(target *url.URL, chain alice.Chain) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = r.transport

	return chain.Then(proxy)
}

func (r *Router) notifySandboxesServiceMiddleware() alice.Constructor {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := req.Context()

			appID, ok := ctx.Value(AppIDCtxKey).(string)
			if !ok {
				// App ID should always be defined when the request gets to this middleware.
				w.WriteHeader(http.StatusBadGateway)
				return
			}

			trace := &httptrace.ClientTrace{
				// Send notification only if a connection to the app was made successfully.
				GotConn: func(connInfo httptrace.GotConnInfo) {
					r.wg.Add(1)
					// Current request should not wait for the notification
					go func() {
						defer r.wg.Done()

						notificationCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()

						notificationCtx = ctxattr.ContextWith(notificationCtx, attribute.String(attrAppID, appID))

						_, span := r.telemetry.Tracer().Start(ctx, "keboola.go.apps-proxy.app.notify")
						notificationCtx = telemetry.ContextWithSpan(notificationCtx, span)

						// Error is already logged by the Notify method itself. We can ignore it here.
						err := r.loader.Notify(notificationCtx, appID) // nolint: contextcheck // intentionally creating new context for background operation
						span.End(&err)
					}()
				},
			}
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

			next.ServeHTTP(w, req)
		})
	}
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

type oauthProvider struct {
	providerConfig options.Provider
	proxyConfig    *options.Options
	proxyProvider  providers.Provider
	handler        http.Handler
}

func (r *Router) createProvider(ctx context.Context, authProvider dataapps.AuthProvider, app dataapps.AppProxyConfig, chain alice.Chain) *oauthProvider {
	authValidator := func(email string) bool {
		// No need to verify users, just groups which is done using AllowedGroups in provider configuration.
		return true
	}

	exceptionID := r.exceptionIDPrefix + idgenerator.RequestID()

	allowedRoles := []string{}
	if authProvider.AllowedRoles != nil {
		allowedRoles = *authProvider.AllowedRoles
	}

	providerConfig := options.Provider{
		ID:                  authProvider.ID,
		Type:                options.ProviderType(authProvider.Type),
		Name:                authProvider.Name,
		AllowedGroups:       allowedRoles,
		CodeChallengeMethod: providers.CodeChallengeMethodS256,
		ClientID:            authProvider.ClientID,
		ClientSecret:        authProvider.ClientSecret,
		OIDCConfig: options.OIDCOptions{
			IssuerURL:      authProvider.IssuerURL,
			EmailClaim:     options.OIDCEmailClaim,
			GroupsClaim:    options.OIDCGroupsClaim,
			AudienceClaims: options.OIDCAudienceClaims,
			UserIDClaim:    options.OIDCEmailClaim,
		},
		BackendLogoutURL: authProvider.LogoutURL,
	}

	// Use error handler by default.
	provider := &oauthProvider{
		providerConfig: providerConfig,
		handler:        r.createConfigErrorHandler(exceptionID),
	}

	// Create a configuration for oauth2-proxy. This can cause a validation failure.
	proxyConfig, err := r.authProxyConfig(app, providerConfig, chain)
	if err != nil {
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unable to create oauth proxy config for app "%s" "%s": %s`, app.ID, app.Name, err.Error())
		return provider
	}
	provider.proxyConfig = proxyConfig

	// AllowedRoles nil means there is no role requirement. Empty array doesn't make sense.
	if authProvider.AllowedRoles != nil && len(allowedRoles) == 0 {
		r.logger.With(attribute.String("exceptionId", exceptionID)).Infof(ctx, `empty array of allowed roles for app "%s" "%s"`, app.ID, app.Name)
		return provider
	}

	// Create a provider instance. This may fail on invalid url, unknown provider type and various other reasons.
	proxyProvider, err := providers.NewProvider(providerConfig)
	if err != nil {
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unable to create oauth provider for app "%s" "%s": %s`, app.ID, app.Name, err.Error())
		return provider
	}
	provider.proxyProvider = proxyProvider

	// Create the actual proxy instance. May fail on some runtime error.
	proxy, err := oauthproxy.NewOAuthProxy(proxyConfig, authValidator)
	if err != nil {
		r.logger.With(attribute.String("exceptionId", exceptionID)).Warnf(ctx, `unable to start oauth proxy for app "%s" "%s": %s`, app.ID, app.Name, err.Error())
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
					Scheme:   r.config.API.PublicURL.Scheme,
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
				providerCookie,
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
func (r *Router) createMultiProviderHandler(oauthProviders map[string]*oauthProvider, app dataapps.AppProxyConfig) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var provider *oauthProvider
		ok := false

		// Identify the provider chosen by the user using a cookie
		cookie, err := request.Cookie(providerCookie)
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
						providerCookie,
						provider.providerConfig.ID,
						&provider.proxyConfig.Cookie,
						provider.proxyConfig.Cookie.Expire,
						r.clock.Now(),
					))
				}

				provider.handler.ServeHTTP(writer, request)
			} else {
				// Clear the provider cookie in case it existed with an invalid value
				opts := &options.NewOptions().Cookie
				opts.Domains = []string{r.formatAppDomain(app)}
				http.SetCookie(writer, cookies.MakeCookieFromOptions(
					request,
					providerCookie,
					"",
					opts,
					time.Hour*-1,
					r.clock.Now(),
				))

				r.redirectToProviderSelection(writer, request)
				writer.WriteHeader(http.StatusFound)
			}

			return
		}

		if provider.proxyProvider != nil {
			loginURL := provider.proxyProvider.Data().LoginURL

			// If oauthproxy returns a redirect to login page, we instead redirect to provider selection page
			writer = NewCallbackResponseWriter(writer, func(writer http.ResponseWriter, statusCode int) int {
				if statusCode != http.StatusFound {
					return statusCode
				}

				locationURL, err := url.Parse(writer.Header().Get("Location"))
				if err != nil {
					return statusCode
				}

				// Redirect to OAuth2 provider is instead redirected to selection page
				if locationURL.Host == loginURL.Host && locationURL.Path == loginURL.Path && len(oauthProviders) > 1 {
					r.redirectToProviderSelection(writer, request)
					return http.StatusFound
				}

				return statusCode
			})
		}

		if request.URL.Path == "/_proxy/sign_out" {
			// Clear the provider cookie on sign out
			opts := &options.NewOptions().Cookie
			opts.Domains = []string{r.formatAppDomain(app)}
			http.SetCookie(writer, cookies.MakeCookieFromOptions(
				request,
				providerCookie,
				"",
				opts,
				time.Hour*-1,
				r.clock.Now(),
			))
		}

		// Authenticate the request by the provider selected in the cookie
		provider.handler.ServeHTTP(writer, request)
	})
}

func (r *Router) redirectToProviderSelection(writer http.ResponseWriter, request *http.Request) {
	selectionPageURL := &url.URL{
		Scheme: r.config.API.PublicURL.Scheme,
		Host:   request.Host,
		Path:   selectionPagePath,
	}

	writer.Header().Set("Location", selectionPageURL.String())
}

func (r *Router) authProxyConfig(app dataapps.AppProxyConfig, provider options.Provider, chain alice.Chain) (*options.Options, error) {
	v := options.NewOptions()

	domain := r.formatAppDomain(app)

	secret, err := r.generateCookieSecret(app, provider)
	if err != nil {
		return v, err
	}

	v.Cookie.Secret = string(secret)
	v.Cookie.Domains = []string{domain}
	v.Cookie.SameSite = "lax"
	v.ProxyPrefix = "/_proxy"
	v.RawRedirectURL = r.config.API.PublicURL.Scheme + "://" + domain + v.ProxyPrefix + "/callback"

	v.Providers = options.Providers{provider}
	v.SkipProviderButton = true
	v.Session = options.SessionOptions{Type: options.CookieSessionStoreType}
	v.EmailDomains = []string{"*"}
	v.InjectRequestHeaders = []options.Header{
		headerFromClaim("X-Kbc-User-Name", "name"),
		headerFromClaim("X-Kbc-User-Email", options.OIDCEmailClaim),
		headerFromClaim("X-Kbc-User-Roles", options.OIDCGroupsClaim),
	}
	v.UpstreamChain = chain
	v.UpstreamServers = options.UpstreamConfig{
		Upstreams: []options.Upstream{
			{
				ID:        app.ID,
				Path:      "/",
				URI:       app.UpstreamAppURL,
				Transport: r.transport,
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

func (r *Router) formatAppDomain(app dataapps.AppProxyConfig) string {
	domain := app.ID + "." + r.config.API.PublicURL.Host
	if app.Name != "" {
		domain = app.Name + "-" + domain
	}
	return domain
}

// generateCookieSecret creates a unique cookie secret for each app and provider.
// This is necessary because otherwise cookies created by provider A would also be valid in a section that requires provider B but not A.
// To solve this we use the combination of the provider id and our cookie secret as a seed for the real cookie secret.
// App ID is also used as part of the seed because cookies for app X cannot be valid for app Y even if they're using the same provider.
func (r *Router) generateCookieSecret(app dataapps.AppProxyConfig, provider options.Provider) ([]byte, error) {
	if r.config.CookieSecretSalt == "" {
		return nil, errors.New("missing cookie secret salt")
	}

	h := sha256.New()
	if _, err := io.WriteString(h, app.ID+"/"+provider.ID+"/"+r.config.CookieSecretSalt); err != nil {
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
