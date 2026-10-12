package oauthproxy

import (
	"testing"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/stretchr/testify/require"
)

func TestValidateEndpointURL(t *testing.T) {
	t.Parallel()

	allowed := []string{
		"https://login.microsoftonline.com/1111-111-1111-11111111/v2.0",
		"https://accounts.google.com",
		"https://gitlab.example.com:8443",
		"https://login.example.com/logout?id_token_hint={id_token}",
	}
	for _, raw := range allowed {
		require.NoError(t, validateEndpointURL(raw), raw)
	}

	rejected := []string{
		"http://accounts.google.com",
		"ftp://accounts.google.com",
		"accounts.google.com",
		"https://",
		"https:///path",
		"https://127.0.0.1",
		"https://169.254.169.254/computeMetadata/v1",
		"https://[::1]:8443",
		"https://10.0.0.1",
		"https://8.8.8.8",
		"https://localhost",
		"https://LOCALHOST:8443",
		"https://app.localhost",
		"https://kubernetes",
		"https://metadata:8080",
		"https://printer.local",
		"https://metadata.google.internal",
		"https://kubernetes.default.svc",
		"https://kubernetes.default.svc.",
		"https://sandboxes-service.default.svc.cluster.local",
		"://bad",
	}
	for _, raw := range rejected {
		require.Error(t, validateEndpointURL(raw), raw)
	}
}

func TestValidateProviderEndpoints(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateProviderEndpoints(options.Provider{
		OIDCConfig:       options.OIDCOptions{IssuerURL: "https://accounts.google.com"},
		BackendLogoutURL: "https://accounts.google.com/Logout",
	}))

	require.NoError(t, validateProviderEndpoints(options.Provider{}))

	cases := []options.Provider{
		{OIDCConfig: options.OIDCOptions{IssuerURL: "https://10.0.0.1"}},
		{BackendLogoutURL: "https://10.0.0.1/logout"},
		{LoginURL: "https://10.0.0.1/login/oauth/authorize"},
		{RedeemURL: "https://10.0.0.1/login/oauth/access_token"},
		{ValidateURL: "https://10.0.0.1/api/v3"},
		{ProfileURL: "https://10.0.0.1/profile"},
	}
	for _, p := range cases {
		require.Error(t, validateProviderEndpoints(p))
	}
}
