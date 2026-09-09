package oauthproxy

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/validation"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func proxyConfig(
	cfg config.Config,
	selector *selector.Selector,
	pageWriter *pagewriter.Writer,
	app api.AppConfig,
	oAuthProvider provider.OAuthProvider,
	upstream chain.Handler,
) (*options.Options, error) {
	// Generate unique cookies secret
	secret, err := generateCookieSecret(cfg, app, oAuthProvider.ID())
	if err != nil {
		return nil, err
	}

	proxyProvider, err := oAuthProvider.ProxyProviderOptions()
	if err != nil {
		return nil, err
	}

	v := options.NewOptions()

	// Connect to the app upstream
	v.UpstreamHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := upstream.ServeHTTPOrError(w, req); err != nil {
			pageWriter.WriteError(w, req, &app, err)
		}
	})

	// Render the selector page, if login is needed, it is not an internal URL
	v.OnNeedsLogin = func(rw http.ResponseWriter, req *http.Request) (stop bool) {
		// Bypass internal paths
		if strings.HasPrefix(req.URL.Path, v.ProxyPrefix) {
			return false
		}

		return selector.OnNeedsLogin(&app, pageWriter.WriteError)(rw, req)
	}
	// Setup
	domain := app.CookieDomain(cfg.API.PublicURL)
	redirectURL := cfg.API.PublicURL.Scheme + "://" + domain + config.InternalPrefix + "/callback"
	v.Logging.RequestIDHeader = config.RequestIDHeader
	v.Logging.RequestEnabled = false // we have log middleware for all requests
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
		// Kept: data apps already read these, keboola_streamlit among them.
		// Session tracking deliberately records only the user id.
		headerFromClaim("X-Kbc-User-Id", userIDClaim(oAuthProvider.Type())),
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
func generateCookieSecret(cfg config.Config, app api.AppConfig, providerID provider.ID) (string, error) {
	if cfg.CookieSecretSalt == "" {
		return "", errors.New("missing cookie secret salt")
	}

	h := sha256.New()
	h.Write([]byte(app.ID.String() + "/" + providerID.String() + "/" + cfg.CookieSecretSalt))
	bs := h.Sum(nil)

	// Result must be 32 chars, 2 hex chars for each byte
	return fmt.Sprintf("%x", bs[:16]), nil
}

// userIDClaim is the claim that identifies the user for a provider.
//
// GitHub issues no ID token, so it has no subject claim to give. Its account
// login is the closest equivalent: scoped to the provider, and not an e-mail
// address. Unlike a subject it is not permanent — a login can be changed, and
// a released one can be taken over by another account.
func userIDClaim(providerType provider.Type) string {
	if providerType == provider.TypeGitHub {
		return "user"
	}
	return "sub"
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
