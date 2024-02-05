package http

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/justinas/alice"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type DataApp struct {
	ID           AppID              `json:"id" validator:"required"`
	Name         string             `json:"name" validator:"required"`
	UpstreamHost string             `json:"upstreamUrl" validator:"required"`
	Providers    []options.Provider `json:"providers"`
}

type AppID string

func (v AppID) String() string {
	return string(v)
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
