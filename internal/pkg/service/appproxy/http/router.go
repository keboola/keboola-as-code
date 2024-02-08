package http

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/justinas/alice"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/cookies"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
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
}

const ProviderCookie = "_oauth2_provider"

const selectionPagePath = "/proxy/selection"

//go:embed template/*
var templates embed.FS

func NewRouter(ctx context.Context, d dependencies.ServiceScope, apps []DataApp) (*Router, error) {
	html, err := templates.ReadFile("template/selection.html.tmpl")
	if err != nil {
		return nil, errors.Wrap(err, "Selection template file not found")
	}

	tmpl, err := template.New("selection template").Parse(string(html))
	if err != nil {
		return nil, errors.Wrap(err, "Could not parse selection template")
	}

	router := &Router{
		logger:            d.Logger(),
		telemetry:         d.Telemetry(),
		config:            d.Config(),
		clock:             d.Clock(),
		handlers:          map[AppID]http.Handler{},
		selectionTemplate: tmpl,
	}

	for _, app := range apps {
		if handler, err := router.createDataAppHandler(app); err == nil {
			router.handlers[app.ID] = handler
		} else {
			router.logger.Errorf(ctx, `cannot initialize application "%s": %s`, app.ID, err)
		}
	}

	return router, nil
}

func (r *Router) CreateHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		appID, ok := parseAppID(req.Host)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `Unable to parse application ID from the URL.`)
			return
		}

		if handler, found := r.handlers[appID]; found {
			handler.ServeHTTP(w, req)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `Application "%s" not found.`, appID)
		}
	})

	return mux
}

func (r *Router) createDataAppHandler(app DataApp) (http.Handler, error) {
	chain := alice.New()
	if len(app.Providers) == 0 {
		return r.publicAppHandler(app, chain)
	} else {
		return r.protectedAppHandler(app, chain)
	}
}

func (r *Router) publicAppHandler(app DataApp, chain alice.Chain) (http.Handler, error) {
	target, err := url.Parse("http://" + app.UpstreamHost)
	if err != nil {
		return nil, errors.Errorf(`cannot parse upstream url "%s" for app %s: %w`, app.UpstreamHost, app.ID, err)
	}
	return chain.Then(httputil.NewSingleHostReverseProxy(target)), nil
}

type oauthProvider struct {
	providerConfig options.Provider
	proxyConfig    *options.Options
	proxyProvider  providers.Provider
	proxy          *oauthproxy.OAuthProxy
}

func (r *Router) protectedAppHandler(app DataApp, chain alice.Chain) (http.Handler, error) {
	authValidator := func(email string) bool {
		// No need to verify users, just groups which is done using AllowedGroups in provider configuration.
		return true
	}

	oauthProviders := make(map[string]oauthProvider)

	for i, providerConfig := range app.Providers {
		proxyConfig, err := r.authProxyConfig(app, providerConfig, chain)
		if err != nil {
			return nil, errors.Errorf("unable to create oauth proxy config for app %s: %w", app.ID, err)
		}

		proxy, err := oauthproxy.NewOAuthProxy(proxyConfig, authValidator)
		if err != nil {
			return nil, errors.Errorf("unable to start oauth proxy for app %s: %w", app.ID, err)
		}

		proxyProvider, err := providers.NewProvider(providerConfig)
		if err != nil {
			return nil, errors.Errorf("unable to create oauth provider for app %s: %w", app.ID, err)
		}

		oauthProviders[strconv.Itoa(i)] = oauthProvider{
			providerConfig: providerConfig,
			proxyConfig:    proxyConfig,
			proxyProvider:  proxyProvider,
			proxy:          proxy,
		}
	}

	if len(app.Providers) == 1 {
		return oauthProviders["0"].proxy, nil
	}

	return r.createMultiProviderHandler(oauthProviders), nil
}

type SelectionPageData struct {
	Providers []SelectionPageProvider
}

type SelectionPageProvider struct {
	Name string
	URL  string
}

// OAuth2 Proxy doesn't support multiple providers despite the possibility of setting them up in configuration.
// So instead we're using separate proxy instance for each provider with a cookie to remember the selection.
// See https://github.com/oauth2-proxy/oauth2-proxy/issues/926
func (r *Router) createMultiProviderHandler(oauthProviders map[string]oauthProvider) http.Handler {
	handler := http.NewServeMux()

	// Request to provider selection page
	handler.HandleFunc(selectionPagePath, func(writer http.ResponseWriter, request *http.Request) {
		selection := request.URL.Query().Get("select")
		provider, ok := oauthProviders[selection]

		if !ok {
			// Render selection page
			data := SelectionPageData{}
			for id, oauthProvider := range oauthProviders {
				providerURL := &url.URL{
					Scheme:   r.config.PublicAddress.Scheme,
					Host:     request.Host,
					Path:     selectionPagePath,
					RawQuery: "select=" + id,
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

		// Set cookie value to the selected provider
		http.SetCookie(writer, cookies.MakeCookieFromOptions(
			request,
			ProviderCookie,
			selection,
			&provider.proxyConfig.Cookie,
			provider.proxyConfig.Cookie.Expire,
			r.clock.Now(),
		))

		// Request using the selected provider to trigger a redirect to the provider's sign in page
		newRequest, err := http.NewRequestWithContext(request.Context(), http.MethodGet, "/", nil)
		if err != nil {
			r.logger.Error(request.Context(), "could not create request")
		}

		provider.proxy.ServeHTTP(writer, newRequest)
	})

	// Request to the data app itself
	handler.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		var provider oauthProvider
		ok := false

		// Identify the provider chosen by the user using a cookie
		cookie, err := request.Cookie(ProviderCookie)
		if err == nil {
			provider, ok = oauthProviders[cookie.Value]
		}

		if !ok {
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

			return
		}

		// If oauthproxy returns a redirect to login page, we instead redirect to provider selection page
		cw := NewCallbackResponseWriter(writer, func(writer http.ResponseWriter, statusCode int) {
			if statusCode != http.StatusFound {
				return
			}

			locationURL, err := url.Parse(writer.Header().Get("Location"))
			if err != nil {
				return
			}

			loginURL := provider.proxyProvider.Data().LoginURL

			// Redirect to OAuth2 provider is instead redirected to selection page
			if locationURL.Host == loginURL.Host && locationURL.Path == loginURL.Path {
				r.redirectToProviderSelection(writer, request)
			}
		})

		// Authenticate the request by the provider selected in the cookie
		provider.proxy.ServeHTTP(cw, request)
	})

	return handler
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

func (r *Router) authProxyConfig(app DataApp, provider options.Provider, chain alice.Chain) (*options.Options, error) {
	v := options.NewOptions()

	domain := app.ID.String() + "." + r.config.PublicAddress.Host

	v.Cookie.Secret = r.config.CookieSecret
	v.Cookie.Domains = []string{domain}
	v.ProxyPrefix = "/proxy"
	v.RawRedirectURL = r.config.PublicAddress.Scheme + "://" + domain + v.ProxyPrefix + "/callback"

	v.Providers = options.Providers{provider}
	v.SkipProviderButton = true
	v.Session = options.SessionOptions{Type: options.CookieSessionStoreType}
	v.EmailDomains = []string{"*"}
	v.InjectRequestHeaders = []options.Header{
		headerFromClaim("X-Forwarded-Email", options.OIDCEmailClaim),
	}
	v.UpstreamChain = chain
	v.UpstreamServers = options.UpstreamConfig{
		Upstreams: []options.Upstream{
			{
				ID:   app.ID.String(),
				Path: "/",
				URI:  "http://" + app.UpstreamHost,
			},
		},
	}

	if err := validation.Validate(v); err != nil {
		return nil, err
	}

	return v, nil
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

func parseAppID(host string) (AppID, bool) {
	if strings.Count(host, ".") != 3 {
		return "", false
	}

	idx := strings.IndexByte(host, '.')
	if idx < 0 {
		return "", false
	}

	subdomain := host[:idx]
	idx = strings.LastIndexByte(subdomain, '-')
	if idx < 0 {
		return AppID(subdomain), true
	}

	return AppID(subdomain[idx+1:]), true
}
