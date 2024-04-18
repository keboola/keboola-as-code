package testutil

import (
	"testing"

	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/require"
)

func StartOIDCProviderServer(t *testing.T) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.Run()
	require.NoError(t, err)

	return m
}
