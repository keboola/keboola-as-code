package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/justinas/alice"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Router struct {
	logger    log.Logger
	telemetry telemetry.Telemetry
	config    config.Config
	handlers  map[AppID]http.Handler
}

func NewRouter(ctx context.Context, d dependencies.ServiceScope, apps []DataApp) *Router {
	router := &Router{
		logger:    d.Logger(),
		telemetry: d.Telemetry(),
		config:    d.Config(),
		handlers:  map[AppID]http.Handler{},
	}

	for _, app := range apps {
		if handler, err := handlerFor(app, router.config); err == nil {
			router.handlers[app.ID] = handler
		} else {
			router.logger.Errorf(ctx, `cannot initialize application "%s": %s`, app.ID, err)
		}
	}

	return router
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

func handlerFor(app DataApp, cfg config.Config) (http.Handler, error) {
	chain := alice.New()
	if len(app.Providers) == 0 {
		return publicAppHandler(app, cfg, chain)
	} else {
		return protectedAppHandler(app, cfg, chain)
	}
}

func publicAppHandler(app DataApp, _ config.Config, chain alice.Chain) (http.Handler, error) {
	target, err := url.Parse("http://" + app.UpstreamHost)
	if err != nil {
		return nil, errors.Errorf(`cannot parse upstream url "%s" for app %s: %w`, app.UpstreamHost, app.ID, err)
	}
	return chain.Then(httputil.NewSingleHostReverseProxy(target)), nil
}

func protectedAppHandler(app DataApp, cfg config.Config, chain alice.Chain) (http.Handler, error) {
	authValidator := func(email string) bool {
		// No need to verify users, just groups which is done using AllowedGroups in provider configuration.
		return true
	}

	config, err := authProxyConfig(app, app.Providers[0], cfg, chain)
	if err != nil {
		return nil, errors.Errorf("unable to create oauth proxy config for app %s: %w", app.ID, err)
	}

	handler, err := oauthproxy.NewOAuthProxy(config, authValidator)
	if err != nil {
		return nil, errors.Errorf("unable to start oauth proxy for app %s: %w", app.ID, err)
	}

	return handler, nil
}

func authProxyConfig(app DataApp, provider options.Provider, cfg config.Config, chain alice.Chain) (*options.Options, error) {
	v := options.NewOptions()

	domain := app.ID.String() + "." + cfg.PublicAddress.Host

	v.Cookie.Secret = cfg.CookieSecret
	v.Cookie.Domains = []string{domain}
	v.ProxyPrefix = "/proxy"
	v.RawRedirectURL = cfg.PublicAddress.Scheme + "://" + domain + v.ProxyPrefix + "/callback"

	v.Providers = options.Providers{provider}
	v.SkipProviderButton = true
	v.Session = options.SessionOptions{Type: options.CookieSessionStoreType}
	v.EmailDomains = []string{"*"}
	v.InjectRequestHeaders = []options.Header{headerFromClaim("X-Forwarded-Email", options.OIDCEmailClaim)}
	v.UpstreamChain = chain
	v.UpstreamServers = options.UpstreamConfig{
		Upstreams: []options.Upstream{{ID: app.ID.String(), Path: "/", URI: "http://" + app.UpstreamHost}},
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
