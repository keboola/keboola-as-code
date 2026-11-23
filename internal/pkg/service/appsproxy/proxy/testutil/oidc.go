package testutil

import (
	"crypto/tls"
	"testing"

	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil/idptest"
)

// StartOIDCProviderServer starts a mock OIDC provider at https://idp-<port>.example.com.
// Clients reach it through idptest.Transport or idptest.DialAddr.
func StartOIDCProviderServer(t *testing.T) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.NewServer(nil)
	require.NoError(t, err)

	ln, _ := idptest.Listen(t)
	tlsConfig := idptest.TLSConfig()
	require.NoError(t, m.Start(tls.NewListener(ln, tlsConfig), tlsConfig))
	return m
}
