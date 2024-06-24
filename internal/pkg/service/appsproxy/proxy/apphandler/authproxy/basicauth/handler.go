package basicauth

import (
	"crypto/sha256"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/util"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	providerCookie  = "_oauth2_provider"
	basicAuthCookie = "proxyBasicAuth"
	formPagePath    = config.InternalPrefix + "/form"
)

type Handler struct {
	logger     log.Logger
	pageWriter *pagewriter.Writer
	clock      clock.Clock
	app        api.AppConfig
	basicAuth  provider.Basic
	upstream   chain.Handler
}

func NewHandler(
	logger log.Logger,
	pageWriter *pagewriter.Writer,
	clock clock.Clock,
	app api.AppConfig,
	auth provider.Basic,
	upstream chain.Handler,
) *Handler {
	return &Handler{
		logger:     logger,
		pageWriter: pageWriter,
		clock:      clock,
		app:        app,
		basicAuth:  auth,
		upstream:   upstream,
	}
}

func (h *Handler) Name() string {
	return h.basicAuth.Name()
}

func (h *Handler) SignInPath() string {
	return formPagePath
}

func (h *Handler) CookieExpiration() time.Duration {
	return 1 * time.Hour
}

func (h *Handler) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	host, _ := util.SplitHostPort(req.Host)
	if host == "" {
		panic(errors.New("host cannot be empty"))
	}

	requestCookie, _ := req.Cookie(basicAuthCookie)

	// req.Form / req.PostForm does not work
	b, err := io.ReadAll(req.Body)
	if err != nil {
		return err
	}

	values, err := url.ParseQuery(string(b))
	if err != nil {
		return err
	}

	// Unset cookie as /_proxy/sign_out was called and enforce login
	if requestCookie != nil && req.URL.Path == selector.SignOutPath {
		requestCookie.Value = ""
		h.setCookie(w, host, -1, requestCookie)
		requestCookie = nil
	}

	// Login page
	if !values.Has("password") && requestCookie == nil {
		h.pageWriter.WriteLoginPage(w, req, &h.app, nil)
		return nil
	}

	p := values.Get("password")
	// Login page with unauthorized alert
	if err := h.isAuthorized(p, requestCookie); err != nil {
		h.logger.Warn(req.Context(), err.Error())
		h.pageWriter.WriteLoginPage(w, req, &h.app, err)
		return nil
	}

	expires := h.CookieExpiration()
	// Skip generating cookie value when already set and verified
	if requestCookie != nil {
		h.setCookie(w, host, expires, requestCookie)
		return h.upstream.ServeHTTPOrError(w, req)
	}

	hash := sha256.New()
	hash.Write([]byte(p))
	hashedValue := hash.Sum(nil)
	v := &http.Cookie{
		Value: string(hashedValue),
	}
	h.setCookie(w, host, expires, v)
	return h.upstream.ServeHTTPOrError(w, req)
}

func (h *Handler) isAuthorized(p string, cookie *http.Cookie) error {
	if err := cookie.Valid(); cookie != nil {
		if err != nil {
			return err
		}

		return nil
	}

	if !h.basicAuth.IsAuthorized(p) {
		return errors.New("wrong password was given")
	}

	return nil
}

func (h *Handler) setCookie(w http.ResponseWriter, host string, expires time.Duration, cookie *http.Cookie) {
	v := &http.Cookie{
		Name:     basicAuthCookie,
		Value:    cookie.Value,
		Path:     "/",
		Domain:   host,
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}

	if expires > 0 {
		// If there is an expiration, set it
		v.Expires = h.clock.Now().Add(expires)
	} else {
		// Otherwise clear the cookie
		v.MaxAge = -1
	}

	http.SetCookie(w, v)
}
