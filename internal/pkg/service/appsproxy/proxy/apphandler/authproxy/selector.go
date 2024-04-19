package authproxy

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/requtil"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	providerCookie     = "_oauth2_provider"
	providerQueryParam = "provider"
	callbackQueryParam = "rd" // value match OAuth2Proxy internals and shouldn't be modified (see AppDirector there)
	selectionPagePath  = "/_proxy/selection"
	signOutPath        = "/_proxy/sign_out"
)

type Selector struct {
	clock      clock.Clock
	config     config.Config
	pageWriter *pagewriter.Writer
}

type SelectorForAppRule struct {
	*Selector
	app      api.AppConfig
	handlers map[provider.ID]*Handler
}

func newSelector(d dependencies) *Selector {
	return &Selector{
		clock:      d.Clock(),
		config:     d.Config(),
		pageWriter: d.PageWriter(),
	}
}

func (s *Selector) For(app api.AppConfig, handlers map[provider.ID]*Handler) (*SelectorForAppRule, error) {
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
// 2. If no handler is selected and the path requires authorization, the status code is StatusForbidden.
func (s *SelectorForAppRule) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	// Clear cookie on logout
	if req.URL.Path == signOutPath {
		s.clearCookie(w, req)
	}

	// Skip selector page, if there is only one provider
	if len(s.handlers) == 1 {
		// The handlers variable is a map, use the first handler via a for cycle
		for _, handler := range s.handlers {
			// Set cookie if needed
			if providerID := s.providerIDFromCookie(req); providerID != handler.Provider().ID() {
				s.setCookie(w, req, handler)
			}
			return handler.ServeHTTPOrError(w, req)
		}
	}

	// Render selector page, if it is accessed directly
	if req.URL.Path == selectionPagePath {
		return s.writeSelectorPage(w, req, http.StatusOK)
	}

	// Identify the chosen provider by the cookie
	if handler := s.handlers[s.providerIDFromCookie(req)]; handler != nil {
		return handler.ServeHTTPOrError(w, req)
	}

	// No matching handler found
	return s.writeSelectorPage(w, req, http.StatusUnauthorized)
}

func (s *SelectorForAppRule) writeSelectorPage(w http.ResponseWriter, req *http.Request, status int) error {
	id := provider.ID(req.URL.Query().Get(providerQueryParam))
	if selected, found := s.handlers[id]; found {
		// Set cookie with the same expiration as other provider cookies
		s.setCookie(w, req, selected)

		// Get path for redirect after sign in, it must not refer to an external URL
		query := make(url.Values)
		callback := req.URL.Query().Get(callbackQueryParam)
		if callback != "" && strings.HasPrefix(callback, "/") {
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
	for _, handler := range s.handlers {
		query := make(url.Values)
		query.Set(providerQueryParam, handler.ID().String())
		if callback != "" {
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

func (s *Selector) redirect(w http.ResponseWriter, req *http.Request, path string, query url.Values) {
	w.Header().Set("Location", s.url(req, path, query).String())
	w.WriteHeader(http.StatusFound)
}

func (s *Selector) url(req *http.Request, path string, query url.Values) *url.URL {
	return &url.URL{Scheme: s.config.API.PublicURL.Scheme, Host: requtil.HostPort(req), Path: path, RawQuery: query.Encode()}
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

func (s *Selector) setCookie(w http.ResponseWriter, req *http.Request, handler *Handler) {
	http.SetCookie(w, s.cookie(req, handler.ID().String(), handler.CookieExpiration()))
}

func (s *Selector) cookie(req *http.Request, value string, expires time.Duration) *http.Cookie {
	v := &http.Cookie{
		Name:     providerCookie,
		Value:    value,
		Path:     "/",
		Domain:   requtil.Host(req),
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
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
