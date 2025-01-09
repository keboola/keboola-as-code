package selector

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/util"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	providerCookie             = "_oauth2_provider"
	providerQueryParam         = "provider"
	continueAuthQueryParam     = "continue_auth"
	callbackQueryParam         = "rd" // value match OAuth2Proxy internals and shouldn't be modified (see AppDirector there)
	selectionPagePath          = config.InternalPrefix + "/selection"
	SignOutPath                = config.InternalPrefix + "/sign_out"
	proxyCallbackPath          = config.InternalPrefix + "/callback"
	ignoreProviderCookieCtxKey = ctxKey("ignoreProviderCookieCtxKey")
	selectorHandlerCtxKey      = ctxKey("selectorHandlerCtxKey")
)

type ctxKey string

type Selector struct {
	clock      clockwork.Clock
	config     config.Config
	pageWriter *pagewriter.Writer
}

type SelectorForAppRule struct {
	*Selector
	app      api.AppConfig
	handlers map[provider.ID]Handler
}

type dependencies interface {
	Clock() clockwork.Clock
	Config() config.Config
	PageWriter() *pagewriter.Writer
}

func New(d dependencies) *Selector {
	return &Selector{
		clock:      d.Clock(),
		config:     d.Config(),
		pageWriter: d.PageWriter(),
	}
}

func (s *Selector) For(app api.AppConfig, handlers map[provider.ID]Handler) (*SelectorForAppRule, error) {
	// Validate handlers count
	if len(handlers) == 0 {
		return nil, svcErrors.NewServiceUnavailableError(errors.New(`no authentication provider found`))
	}

	return &SelectorForAppRule{Selector: s, app: app, handlers: handlers}, nil
}

// ServeHTTPOrError renders selector page if there is more than one authentication handler,
// and no handler is selected, or the selected handler is not allowed for the requested path (see api.AuthRule).
//
// The selector page is rendered:
// 1. If it is accessed directly using selectionPagePath, the status code is StatusOK.
// 2. If no handler is selected and the path requires authorization, the status code is StatusUnauthorized.
func (s *SelectorForAppRule) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	// To make the same site strict cookie work we need to replace the redirect from the auth provider with a page that does the redirect.
	if req.URL.Path == proxyCallbackPath {
		query := req.URL.Query()
		if query.Get(continueAuthQueryParam) != "true" {
			query.Set(continueAuthQueryParam, "true")
			baseURL := s.app.BaseURL(s.config.API.PublicURL)
			redirectURL := baseURL.ResolveReference(&url.URL{Path: req.URL.Path, RawQuery: query.Encode()})
			s.pageWriter.WriteRedirectPage(w, req, http.StatusOK, redirectURL.String())
			return nil
		}
	}

	// Store the selector to the context.
	// It is used by the OnNeedsLogin callback, to render the selector page, if the provider needs login.
	// Internal paths (it includes sing in) are bypassed, see Manager.proxyConfig for details.
	req = req.WithContext(context.WithValue(req.Context(), selectorHandlerCtxKey, s))

	// Clear cookie on logout
	if req.URL.Path == SignOutPath {
		// This clears provider selection cookie while oauth2-proxy clears the session cookie.
		// The user isn't logged out on the provider's side, but when redirected to the provider they're
		// forced to select their account again because of the "select_account" flag in LoginURLParameters.
		s.clearCookie(w, req)
	}

	// Render selector page, if it is accessed directly
	if req.URL.Path == selectionPagePath {
		return s.writeSelectorPage(w, req, http.StatusOK)
	}

	// Skip selector page, if there is only one provider
	if len(s.handlers) == 1 {
		// The handlers variable is a map, use the first handler via a for cycle
		for id, handler := range s.handlers {
			// Set cookie if needed
			if providerID := s.providerIDFromCookie(req); providerID != id {
				s.setCookie(w, req, id, handler)
			}

			// Get path for redirect after sign in, it must not refer to an external URL
			callback := req.URL.Path
			if isAcceptedCallbackURL(callback) {
				req.Header.Set(callbackQueryParam, callback)
			}

			return handler.ServeHTTPOrError(w, req)
		}
	}

	// Ignore cookie if we have already tried this provider, but the provider requires login.
	providerID := s.providerIDFromCookie(req)
	if ignore, _ := req.Context().Value(ignoreProviderCookieCtxKey).(bool); ignore {
		providerID = ""
	}

	// Identify the chosen provider by the cookie
	if handler := s.handlers[providerID]; handler != nil {
		return handler.ServeHTTPOrError(w, req)
	}

	// No matching handler found
	return s.writeSelectorPage(w, req, http.StatusUnauthorized)
}

func (s *SelectorForAppRule) writeSelectorPage(w http.ResponseWriter, req *http.Request, status int) error {
	// Mark provider selected
	id := provider.ID(req.URL.Query().Get(providerQueryParam))
	if selected, found := s.handlers[id]; found {
		// Set cookie with the same expiration as other provider cookies
		s.setCookie(w, req, id, selected)

		// Get path for redirect after sign in, it must not refer to an external URL
		query := make(url.Values)
		callback := req.URL.Query().Get(callbackQueryParam)
		if isAcceptedCallbackURL(callback) {
			query.Set(callbackQueryParam, callback)
		}

		// Render sign in page, set callback after login
		s.redirect(w, req, selected.SignInPath(), query)
		return nil
	}

	// Render the page, if there is no cookie or the value is invalid
	s.pageWriter.WriteSelectorPage(w, req, status, s.selectorPageData(req))
	return nil
}

func (s *SelectorForAppRule) selectorPageData(req *http.Request) *pagewriter.SelectorPageData {
	// Pass link back to the current page, if reasonable, otherwise the user will be redirected to /
	var callback string
	if req.Method == http.MethodGet {
		callback = req.URL.Path
	}

	// Base URL for all providers
	pageURL := s.url(req, selectionPagePath, nil)

	// Generate link for each providers
	data := &pagewriter.SelectorPageData{App: pagewriter.NewAppData(&s.app)}
	for id, handler := range s.handlers {
		query := make(url.Values)
		query.Set(providerQueryParam, id.String())
		if isAcceptedCallbackURL(callback) {
			query.Set(callbackQueryParam, callback)
		}
		data.Providers = append(data.Providers, pagewriter.ProviderData{
			Name: handler.Name(),
			URL:  pageURL.ResolveReference(&url.URL{RawQuery: query.Encode()}).String(),
		})
	}

	// Sort items
	slices.SortStableFunc(data.Providers, func(a, b pagewriter.ProviderData) int {
		return strings.Compare(a.Name, b.Name)
	})

	return data
}

func (s *Selector) OnNeedsLogin(
	app *api.AppConfig,
	writeErrorPage func(rw http.ResponseWriter, req *http.Request, app *api.AppConfig, err error),
) func(rw http.ResponseWriter, req *http.Request) bool {
	return func(w http.ResponseWriter, req *http.Request) (stop bool) {
		// Determine, if we should render the selector page using the selector instance from the context
		if selector, ok := req.Context().Value(selectorHandlerCtxKey).(*SelectorForAppRule); ok {
			// If there is only one provider, continue to the sing in page
			if len(selector.handlers) <= 1 {
				return false
			}

			// Go back and render the selector page, ignore the cookie value
			req = req.WithContext(context.WithValue(req.Context(), ignoreProviderCookieCtxKey, true))
			if err := selector.ServeHTTPOrError(w, req); err != nil {
				writeErrorPage(w, req, app, err)
			}
			return true
		}

		// Fallback, the selector instance is not found, it shouldn't happen.
		// Clear the cookie and redirect to the same path, so the selector page is rendered.
		s.clearCookie(w, req)
		w.Header().Set("Location", req.URL.Path)
		w.WriteHeader(http.StatusFound)
		return true
	}
}

func (s *Selector) redirect(w http.ResponseWriter, req *http.Request, path string, query url.Values) {
	w.Header().Set("Location", s.url(req, path, query).String())
	w.WriteHeader(http.StatusFound)
}

func (s *Selector) url(req *http.Request, path string, query url.Values) *url.URL {
	return &url.URL{Scheme: s.config.API.PublicURL.Scheme, Host: req.Host, Path: path, RawQuery: query.Encode()}
}

func (s *Selector) providerIDFromCookie(req *http.Request) provider.ID {
	if cookie, _ := req.Cookie(providerCookie); cookie != nil && cookie.Value != "" {
		return provider.ID(cookie.Value)
	}
	return ""
}

func (s *Selector) clearCookie(w http.ResponseWriter, req *http.Request) {
	http.SetCookie(w, s.cookie(req, "", -1))
}

func (s *Selector) setCookie(w http.ResponseWriter, req *http.Request, id provider.ID, handler Handler) {
	http.SetCookie(w, s.cookie(req, id.String(), handler.CookieExpiration()))
}

func (s *Selector) cookie(req *http.Request, value string, expires time.Duration) *http.Cookie {
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
	return callback != "" && callback != "/" && !strings.HasPrefix(callback, config.InternalPrefix)
}
