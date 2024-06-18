package basicauth

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/util"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

const (
	providerCookie      = "_basicauth_provider"
	providerQueryParam  = "provider"
	promptPagePath      = config.InternalPrefix + "/password"
	signOutPath         = config.InternalPrefix + "/sign_out"
	promptHandlerCtxKey = ctxKey("promptHandlerCtxKey")
)

type ctxKey string

type Prompt struct {
	clock      clock.Clock
	config     config.Config
	pageWriter *pagewriter.Writer
	app        api.AppConfig
	handler    *Handler
}

/*type Prompt struct {
	*Selector
	app      api.AppConfig
	handlers map[provider.ID]*Handler
}*/

// ServeHTTPOrError renders selector page if there is more than one authentication handler,
// and no handler is selected, or the selected handler is not allowed for the requested path (see api.AuthRule).
//
// The selector page is rendered:
// 1. If it is accessed directly using selectionPagePath, the status code is StatusOK.
// 2. If no handler is selected and the path requires authorization, the status code is StatusUnauthorized.
func (s *Prompt) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	// Store the selector to the context.
	// It is used by the OnNeedsLogin callback, to render the selector page, if the provider needs login.
	// Internal paths (it includes sing in) are bypassed, see Manager.proxyConfig for details.
	req = req.WithContext(context.WithValue(req.Context(), promptHandlerCtxKey, s))

	// Clear cookie on logout
	if req.URL.Path == signOutPath {
		// This clears provider selection cookie while oauth2-proxy clears the session cookie.
		// The user isn't logged out on the provider's side, but when redirected to the provider they're
		// forced to select their account again because of the "select_account" flag in LoginURLParameters.
		s.clearCookie(w, req)
	}

	// Render selector page, if it is accessed directly
	if req.URL.Path == promptPagePath {
		return s.writeSelectorPage(w, req, http.StatusOK)
	}

	// Set cookie if needed
	/*if providerID := s.providerIDFromCookie(req); providerID != handler.Provider().ID() {
		s.setCookie(w, req, handler)
	}
	return handler.ServeHTTPOrError(w, req)*/

	// No matching handler found
	return s.writeSelectorPage(w, req, http.StatusUnauthorized)
}

func (s *Prompt) writeSelectorPage(w http.ResponseWriter, req *http.Request, status int) error {
	// Mark provider selected
	id := provider.ID(req.URL.Query().Get(providerQueryParam))
	// Set cookie with the same expiration as other provider cookies
	if s.handler != nil && s.handler.ID() == id {
		s.setCookie(w, req)

		// Get path for redirect after sign in, it must not refer to an external URL
		query := make(url.Values)
		/*callback := req.URL.Query().Get(callbackQueryParam)
		if isAcceptedCallbackURL(callback) {
			query.Set(callbackQueryParam, callback)
		}*/

		// Render sign in page, set callback after login
		s.redirect(w, req, "/sign_in", query)
		return nil
	}
	// Render the page, if there is no cookie or the value is invalid
	return nil
}

func (s *Prompt) promptPageData(req *http.Request) *pagewriter.PromptPageData {
	// Pass link back to the current page, if reasonable, otherwise the user will be redirected to /
	var callback string
	if req.Method == http.MethodGet {
		callback = req.URL.Path
	}

	// Base URL for all providers
	pageURL := s.url(req, promptPagePath, nil)
	_ = pageURL

	// Generate link for each providers
	data := &pagewriter.PromptPageData{App: pagewriter.NewAppData(&s.app)}
	query := make(url.Values)
	query.Set(providerQueryParam, s.handler.ID().String())
	_ = callback
	/*if isAcceptedCallbackURL(callback) {
		query.Set(callbackQueryParam, callback)
	}*/

	return data
}

func (s *Prompt) redirect(w http.ResponseWriter, req *http.Request, path string, query url.Values) {
	w.Header().Set("Location", s.url(req, path, query).String())
	w.WriteHeader(http.StatusFound)
}

func (s *Prompt) url(req *http.Request, path string, query url.Values) *url.URL {
	return &url.URL{Scheme: s.config.API.PublicURL.Scheme, Host: req.Host, Path: path, RawQuery: query.Encode()}
}

func (s *Prompt) providerIDFromCookie(req *http.Request) provider.ID {
	if cookie, _ := req.Cookie(providerCookie); cookie != nil && cookie.Value != "" {
		return provider.ID(cookie.Value)
	}
	return ""
}

func (s *Prompt) clearCookie(w http.ResponseWriter, req *http.Request) {
	http.SetCookie(w, s.cookie(req, "", -1))
}

func (s *Prompt) setCookie(w http.ResponseWriter, req *http.Request) {
	http.SetCookie(w, s.cookie(req, s.handler.ID().String(), s.handler.CookieExpiration()))
}

func (s *Prompt) cookie(req *http.Request, value string, expires time.Duration) *http.Cookie {
	host, _ := util.SplitHostPort(req.Host)
	if host == "" {
		panic(errors.New("host cannot be empty"))
	}

	v := &http.Cookie{
		Name:     providerCookie,
		Value:    value,
		Path:     "/",
		Domain:   host,
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}

	if expires > 0 {
		// If there is an expiration, set it
		v.Expires = s.clock.Now().Add(expires)
	} else {
		// Otherwise clear the cookie
		v.MaxAge = -1
	}

	return v
}

func isAcceptedCallbackURL(callback string) bool {
	return callback != "" && callback != "/" && callback != promptPagePath && strings.HasPrefix(callback, "/")
}
