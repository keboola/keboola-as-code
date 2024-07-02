package basicauth

import (
	"crypto/sha256"
	"encoding/hex"
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
	basicAuthCookie    = "proxyBasicAuth"
	callbackQueryParam = "rd" // value match OAuth2Proxy internals and shouldn't be modified (see AppDirector there)
	formPagePath       = config.InternalPrefix + "/form"
)

type Handler struct {
	logger     log.Logger
	pageWriter *pagewriter.Writer
	clock      clock.Clock
	publicURL  *url.URL
	app        api.AppConfig
	basicAuth  provider.Basic
	upstream   chain.Handler
}

func NewHandler(
	logger log.Logger,
	pageWriter *pagewriter.Writer,
	clock clock.Clock,
	publicURL *url.URL,
	app api.AppConfig,
	auth provider.Basic,
	upstream chain.Handler,
) *Handler {
	return &Handler{
		logger:     logger,
		pageWriter: pageWriter,
		clock:      clock,
		publicURL:  publicURL,
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
		return errors.New("internal server error")
	}

	requestCookie, _ := req.Cookie(basicAuthCookie)
	// Pass request to upstream when cookie have been set
	if requestCookie != nil && req.URL.Path != selector.SignOutPath && h.isCookieAuthorized(requestCookie) == nil {
		h.setCookie(w, host, h.CookieExpiration(), requestCookie)
		return h.upstream.ServeHTTPOrError(w, req)
	}

	// Unset cookie as /_proxy/sign_out was called and enforce login by redirecting to SignInPath
	if requestCookie != nil && req.URL.Path == selector.SignOutPath {
		h.signOut(host, requestCookie, w, req)
		return nil
	}

	if err := req.ParseForm(); err != nil {
		return err
	}

	// Login page
	if !req.Form.Has("password") && requestCookie == nil {
		h.pageWriter.WriteLoginPage(w, req, &h.app, nil)
		return nil
	}

	p := req.Form.Get("password")
	// Login page with unauthorized alert
	if err := h.isAuthorized(p, requestCookie); err != nil {
		h.logger.Warn(req.Context(), err.Error())
		h.pageWriter.WriteLoginPage(w, req, &h.app, err)
		return nil
	}

	// Redirect to page that user comes from
	path := req.Header.Get(callbackQueryParam)
	if path == "" {
		path = "/"
	}

	redirectURL := &url.URL{
		Scheme: h.publicURL.Scheme,
		Host:   req.Host,
		Path:   path,
	}
	hash := sha256.New()
	hash.Write([]byte(p + string(h.app.ID)))
	hashedValue := hash.Sum(nil)
	v := &http.Cookie{
		Value: hex.EncodeToString(hashedValue),
	}
	h.setCookie(w, host, h.CookieExpiration(), v)
	// Redirect to upstream
	w.Header().Set("Location", redirectURL.String())
	w.WriteHeader(http.StatusMovedPermanently)
	return nil
}

func (h *Handler) isAuthorized(password string, cookie *http.Cookie) error {
	if password != "" && !h.basicAuth.IsAuthorized(password) {
		return errors.New("Please enter a correct password.")
	}

	return h.isCookieAuthorized(cookie)
}

func (h *Handler) isCookieAuthorized(cookie *http.Cookie) error {
	if err := cookie.Valid(); cookie != nil && err != nil {
		return err
	}

	if cookie != nil {
		hash := sha256.New()
		hash.Write([]byte(h.basicAuth.Password + string(h.app.ID)))
		hashedValue := hash.Sum(nil)
		if hex.EncodeToString(hashedValue) != cookie.Value {
			return errors.New("Cookie has expired.")
		}
	}

	return nil
}

func (h *Handler) signOut(host string, cookie *http.Cookie, w http.ResponseWriter, req *http.Request) {
	cookie.Value = ""
	h.setCookie(w, host, -1, cookie)
	redirectURL := &url.URL{
		Scheme: h.publicURL.Scheme,
		Host:   req.Host,
		Path:   h.SignInPath(),
	}
	w.Header().Set("Location", redirectURL.String())
	w.WriteHeader(http.StatusFound)
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
