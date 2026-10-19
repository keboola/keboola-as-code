package apptoken

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
)

func TestWithIdentityProvider(t *testing.T) {
	t.Parallel()

	idp := IdentityProvider{URL: "https://idp.example.com", EmailVerifiedByProvider: true}
	var got IdentityProvider
	var found bool
	next := chain.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) error {
		got, found = identityProviderFromContext(req.Context())
		return nil
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	require.NoError(t, WithIdentityProvider(next, idp).ServeHTTPOrError(httptest.NewRecorder(), req))
	assert.True(t, found)
	assert.Equal(t, idp, got)

	_, found = identityProviderFromContext(t.Context())
	assert.False(t, found)
}
