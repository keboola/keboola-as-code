package testutil

import (
	"fmt"
	"net"
	"slices"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/server"
)

func StartOIDCProviderServer(t *testing.T, pm server.PortManager) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.NewServer(nil)
	if err != nil {
		panic("unable to open mockoidc server" + err.Error())
	}

	port := pm.GetFreePort()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	for err != nil {
		port = pm.GetFreePort()
		ln, err = lc.Listen(t.Context(), "tcp", fmt.Sprintf("[::1]:%d", port))
	}

	require.NoError(t, m.Start(ln, nil))
	return m
}

// MockUser is a mockoidc.MockUser whose groups claim does not depend on the "groups" scope,
// like an IdP configured to always add the claim to the ID token.
type MockUser mockoidc.MockUser

func (u *MockUser) ID() string {
	return (*mockoidc.MockUser)(u).ID()
}

func (u *MockUser) Userinfo(scope []string) ([]byte, error) {
	return (*mockoidc.MockUser)(u).Userinfo(append(slices.Clone(scope), "groups"))
}

func (u *MockUser) Claims(scope []string, claims *mockoidc.IDTokenClaims) (jwt.Claims, error) {
	return (*mockoidc.MockUser)(u).Claims(append(slices.Clone(scope), "groups"), claims)
}
