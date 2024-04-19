package authproxy

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"

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
		if !strings.HasPrefix(v.ProxyPrefix, req.URL.Path) && m.providerSelector.providerIDFromCookie(req) != "" {
			m.providerSelector.clearCookie(w, req)
			w.Header().Set("Location", req.URL.Path)
			w.WriteHeader(http.StatusFound)
			return true
		}
		return false
	}

	// Setup
	domain := app.Domain() + "." + m.config.API.PublicURL.Host
	v.Cookie.Secret = secret
	v.Cookie.Domains = []string{domain}
	v.Cookie.SameSite = "lax"
	v.ProxyPrefix = "/_proxy"
	v.RawRedirectURL = m.config.API.PublicURL.Scheme + "://" + domain + v.ProxyPrefix + "/callback"
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
