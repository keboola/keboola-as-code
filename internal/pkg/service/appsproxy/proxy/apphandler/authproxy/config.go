package authproxy

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Manager) proxyConfig(app api.AppConfig, authProvider provider.Provider, upstream chain.Handler) (*options.Options, error) {
	// Generate unique cookies secret
	secret, err := m.generateCookieSecret(app, authProvider.ID())
	if err != nil {
		return nil, err
	}

	// Generate OAuth2Proxy settings
	proxyProvider, err := authProvider.ToProxyProvider()
	if err != nil {
		return nil, err
	}

	v := options.NewOptions()

	// Connect to the app upstream
	v.UpstreamHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := upstream.ServeHTTPOrError(w, req); err != nil {
			m.pageWriter.WriteError(w, req, &app, err)
		}
	})

	// Render the selector page, if login is needed, it is not an internal URL
	v.OnNeedsLogin = func(w http.ResponseWriter, req *http.Request) (stop bool) {
		// Bypass internal paths
		if strings.HasPrefix(req.URL.Path, v.ProxyPrefix) {
			return false
		}

		// Determine, if we should render the selector page using the selector instance from the context
		if selector, ok := req.Context().Value(selectorHandlerCtxKey).(*SelectorForAppRule); ok {
			// If there is only one provider, continue to the sing in page
			if len(selector.handlers) <= 1 {
				return false
			}

			// Go back and render the selector page, ignore the cookie value
			req = req.WithContext(context.WithValue(req.Context(), ignoreProviderCookieCtxKey, true))
			if err := selector.ServeHTTPOrError(w, req); err != nil {
				m.pageWriter.WriteError(w, req, &app, err)
			}
			return true
		}

		// Fallback, the selector instance is not found, it shouldn't happen.
		// Clear the cookie and redirect to the same path, so the selector page is rendered.
		m.providerSelector.clearCookie(w, req)
		w.Header().Set("Location", req.URL.Path)
		w.WriteHeader(http.StatusFound)
		return true
	}

	// Setup
	domain := app.CookieDomain(m.config.API.PublicURL)
	redirectURL := m.config.API.PublicURL.Scheme + "://" + domain + config.InternalPrefix + "/callback"
	v.Logging.RequestIDHeader = config.RequestIDHeader
	v.Cookie.Secret = secret
	v.Cookie.Domains = []string{domain}
	v.Cookie.SameSite = "strict"
	v.ProxyPrefix = config.InternalPrefix
	v.RawRedirectURL = redirectURL
	v.Providers = options.Providers{proxyProvider}
	v.SkipProviderButton = true
	v.Session = options.SessionOptions{Type: options.CookieSessionStoreType}
	v.EmailDomains = []string{"*"}
	v.InjectRequestHeaders = []options.Header{
		headerFromClaim("X-Kbc-User-Name", "name"),
		headerFromClaim("X-Kbc-User-Email", options.OIDCEmailClaim),
		headerFromClaim("X-Kbc-User-Roles", options.OIDCGroupsClaim),
	}

	// Cannot separate errors from info because when ErrToInfo is false (default),
	// oauthproxy keeps forcibly setting its global error writer to os.Stderr whenever a new proxy instance is created.
	v.Logging.ErrToInfo = true

	if err := validation.Validate(v); err != nil {
		return nil, err
	}

	return v, nil
}

// generateCookieSecret creates a unique cookie secret for each app and provider.
// This is necessary because otherwise cookies created by provider A would also be valid in a section that requires provider B but not A.
// To solve this we use the combination of the provider id and our salt.
func (m *Manager) generateCookieSecret(app api.AppConfig, providerID provider.ID) (string, error) {
	if m.config.CookieSecretSalt == "" {
		return "", errors.New("missing cookie secret salt")
	}

	h := sha256.New()
	h.Write([]byte(app.ID.String() + "/" + providerID.String() + "/" + m.config.CookieSecretSalt))
	bs := h.Sum(nil)

	// Result must be 32 chars, 2 hex chars for each byte
	return fmt.Sprintf("%x", bs[:16]), nil
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
