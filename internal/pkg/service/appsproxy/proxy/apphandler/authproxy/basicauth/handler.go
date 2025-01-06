package basicauth

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/util"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/xsrftoken"

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
	csrfTokenKey       = "_csrf"
	csrfTokenMaxAge    = 15 * time.Minute
)

type Handler struct {
	logger        log.Logger
	clock         clockwork.Clock
	pageWriter    *pagewriter.Writer
	csrfTokenSalt string
	publicURL     *url.URL
	app           api.AppConfig
	basicAuth     provider.Basic
	upstream      chain.Handler
}

func NewHandler(
	logger log.Logger,
	config config.Config,
	clock clockwork.Clock,
	pageWriter *pagewriter.Writer,
	app api.AppConfig,
	auth provider.Basic,
	upstream chain.Handler,
) *Handler {
	return &Handler{
		logger:        logger,
		clock:         clock,
		pageWriter:    pageWriter,
		csrfTokenSalt: config.CsrfTokenSalt,
		publicURL:     config.API.PublicURL,
		app:           app,
		basicAuth:     auth,
		upstream:      upstream,
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

// ServeHTTPOrError serves basic authorization pages based on these conditions:
// nothing || cookie == nil -> render Login page GET/POST (200), no error
// form[password=""] -> Please enter a correct password (200), error
// form[password="b"] -> Please enter a correct password (200), error
// form[password="a"] -> Authorized, set cookie and redirect to URL of user. (301), no error
// cookie != nil && not signout && authorized -> Go to upstream (Based on app state), no error
// cookie != nil && signout -> unset cookie, redirect to Login pageWriter (303), go to _proxy/SignInPath, no error
// cookie != nil && unauthorized -> Cookie has expired (200), error.
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

	// CSRF token validation
	if key := req.Form.Get(csrfTokenKey); key != "" && !xsrftoken.ValidFor(key, csrfTokenKey, h.csrfTokenSalt, "/", csrfTokenMaxAge) {
		h.pageWriter.WriteErrorPage(w, req, &h.app, http.StatusForbidden, "Session expired, reload page", pagewriter.ExceptionIDPrefix+key)
		return nil
	}

	// Login page first access
	csrfToken := xsrftoken.Generate(csrfTokenKey, h.csrfTokenSalt, "/")
	fragment := fmt.Sprintf(`<input type="hidden" name="%s" value="%s">`, template.HTMLEscapeString(csrfTokenKey), template.HTMLEscapeString(csrfToken))
	// This represents `GET` request for `form`
	if !req.Form.Has("password") && requestCookie == nil {
		h.pageWriter.WriteLoginPage(w, req, &h.app, template.HTML(fragment), nil) // #nosec G203 The used method does not auto-escape HTML. This can potentially lead to 'Cross-site Scripting' vulnerabilities
		return nil
	}

	// Login page with unauthorized alert
	p := req.Form.Get("password")
	if err := h.isAuthorized(p, requestCookie); err != nil {
		h.logger.Warn(req.Context(), err.Error())
		h.pageWriter.WriteLoginPage(w, req, &h.app, template.HTML(fragment), err) // #nosec G203 The used method does not auto-escape HTML. This can potentially lead to 'Cross-site Scripting' vulnerabilities
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
	hash, err := bcrypt.GenerateFromPassword([]byte(p+string(h.app.ID)), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	v := &http.Cookie{
		Value: string(hash),
	}
	h.setCookie(w, host, h.CookieExpiration(), v)
	// Redirect to upstream (Same handler)
	w.Header().Set("Location", redirectURL.String())
	w.WriteHeader(http.StatusMovedPermanently)
	return nil
}

func (h *Handler) isAuthorized(password string, cookie *http.Cookie) error {
	// When password was not provided and cookie is set to wrong password, return `Cookie has expired.`
	if cookie != nil && password == "" {
		return h.isCookieAuthorized(cookie)
	}

	// When no password provided, or wrong password is provided against configured one, return error
	if password == "" || !h.basicAuth.IsAuthorized(password) {
		return errors.New("Please enter a correct password.")
	}

	return nil
}

func (h *Handler) isCookieAuthorized(cookie *http.Cookie) error {
	if err := cookie.Valid(); cookie != nil && err != nil {
		return err
	}

	if cookie != nil {
		if err := bcrypt.CompareHashAndPassword([]byte(cookie.Value), []byte(h.basicAuth.Password+string(h.app.ID))); err != nil {
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
