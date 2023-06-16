package main

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"
)

const (
	readHeaderTimeout       = 10 * time.Second
	gracefulShutdownTimeout = 30 * time.Second
)

type Proxy struct {
	logger     log.Logger
	listenAddr string
	publicAddr string
	handlers   map[AppID]http.Handler
}

func main() {
	if err := run(); err != nil {
		fmt.Println(errors.PrefixError(err, "fatal error").Error()) // nolint:forbidigo
		os.Exit(1)
	}
}

func run() error {
	// Dependencies
	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewCliLogger(os.Stdout, os.Stderr, nil, false)
	proc, err := servicectx.New(ctx, cancel)
	if err != nil {
		return err
	}

	// TMP: start OIDC provider
	if err := startOIDCProvider(ctx, "http://localhost:1234", "0.0.0.0:1234", logger, proc); err != nil {
		return err
	}

	// Init proxy struct
	p := &Proxy{
		logger:     logger,
		listenAddr: "0.0.0.0:8000",
		publicAddr: "http://localhost:8000",
		handlers:   make(map[AppID]http.Handler),
	}

	// Init apps
	for _, app := range exampleApps() {
		startStaticHTTPServer(app.Name, app.UpstreamHost, logger, proc) // TMP: - run apps HTTP servers
		p.upsertApp(app)
	}

	// Start proxy HTTP server
	p.start(proc)

	// Wait for HTTP server shutdown
	proc.WaitForShutdown()
	return nil
}

func (p *Proxy) start(proc *servicectx.Process) {
	p.logger.Infof(`Starting proxy HTTP server "%s" ...`, p.listenAddr)

	router := p.buildRouter()
	srv := &http.Server{Addr: p.listenAddr, Handler: router, ReadHeaderTimeout: readHeaderTimeout}

	proc.Add(func(ctx context.Context, shutdown servicectx.ShutdownFn) {
		p.logger.Infof("HTTP server listening on %q", p.listenAddr)
		shutdown(srv.ListenAndServe())
	})

	proc.OnShutdown(func() {
		p.logger.Infof("shutting down HTTP server at %q", p.listenAddr)

		ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			p.logger.Errorf(`HTTP server shutdown error: %s`, err)
		}

		p.logger.Info("HTTP server shutdown finished")
	})
}

// buildRouter to route requests by the AppID.
func (p *Proxy) buildRouter() http.Handler {
	r := mux.NewRouter()

	r.Path("/").Handler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(fmt.Sprintf(`Please specify application ID in the URL in format "/{appId}/...".`)))
	}))

	r.PathPrefix("/{appID}").Handler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		appID := vars["appID"]
		if handler, found := p.handlers[AppID(appID)]; found {
			handler.ServeHTTP(w, req)
		} else {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(fmt.Sprintf(`Application "%s" not found.`, appID)))
		}
	}))

	return r
}

func (p *Proxy) upsertApp(app App) {
	if handler, err := p.handlerFor(app); err == nil {
		p.handlers[app.ID] = handler
	} else {
		p.logger.Errorf(`cannot initialize application "%s": %s`, app.ID, err)
	}
}

func (p *Proxy) handlerFor(app App) (http.Handler, error) {
	chain := alice.New(wakeUpMiddleware(app), trimAppIDMiddleware(app))
	if app.Provider == nil {
		return p.publicAppHandler(app, chain)
	} else {
		return p.protectedAppHandler(app, chain)
	}
}

func (p *Proxy) publicAppHandler(app App, chain alice.Chain) (http.Handler, error) {
	target, err := url.Parse("http://" + app.UpstreamHost)
	if err != nil {
		return nil, errors.Errorf(`cannot parse upstream url "%s": %w`, app.UpstreamHost, err)
	}
	return chain.Then(httputil.NewSingleHostReverseProxy(target)), nil
}

func (p *Proxy) protectedAppHandler(app App, chain alice.Chain) (http.Handler, error) {
	authValidator := func(email string) bool {
		return true
	}

	config, err := authProxyConfig(p.publicAddr, chain, app)
	if err != nil {
		return nil, err
	}

	handler, err := oauthproxy.NewOAuthProxy(config, authValidator)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

// trimAppIDMiddleware removes app ID from the URL before upstream request.
func trimAppIDMiddleware(app App) alice.Constructor {
	return func(next http.Handler) http.Handler {
		return http.StripPrefix("/"+app.ID.String(),
			http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				// Fix empty path
				if req.URL.Path == "" {
					req.URL.Path = "/"
				}

				// OAuth2 Proxy uses internally RequestURI instead of URL field,
				// so both must match the rewritten path
				req.RequestURI = req.URL.RequestURI()

				next.ServeHTTP(w, req)
			}),
		)
	}
}

// wakeUpMiddleware ensures the app has running at least one replica and wait for scale-up if needed.
func wakeUpMiddleware(app App) alice.Constructor {
	return func(next http.Handler) http.Handler {
		isRunning := make(map[AppID]bool)
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Fake waiting for scale-up
			if !isRunning[app.ID] {
				time.Sleep(5 * time.Second)
				isRunning[app.ID] = true
			}
			next.ServeHTTP(w, req)
		})
	}
}

func authProxyConfig(proxyAddr string, chain alice.Chain, app App) (*options.Options, error) {
	v := options.NewOptions()

	v.Cookie.Secret = "s1pvxjhx0NHl7OL5k_zHsfMj-sa0_q-neSL-Xg8T8W0=" // TODO, set from ENV

	v.ProxyPrefix = "/" + app.ID.String() + "/oauth2"
	v.Cookie.Path = "/" + app.ID.String()
	v.RawRedirectURL = proxyAddr + "/" + v.ProxyPrefix + "/callback"

	v.Providers = options.Providers{*app.Provider}
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
