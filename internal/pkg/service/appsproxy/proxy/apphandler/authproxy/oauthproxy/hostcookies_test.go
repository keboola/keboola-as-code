package oauthproxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

func TestHandler_LegacyTicketKeepsTheHostTicketTimestamp(t *testing.T) {
	t.Parallel()

	issuer := testDiscoveryServer(t, nil)
	cfg, app, _, _ := testProxyConfigEnv(t, `[]`)
	pw, err := pagewriter.New(pageWriterDeps{})
	require.NoError(t, err)
	h := NewHandler(t.Context(), log.NewDebugLogger(), client.New(), cfg, testSessionStore(t), nil, pw, app, testOIDC(issuer, nil), nil)
	require.NoError(t, h.initErr)
	secret, err := h.proxyConfig.Cookie.GetSecret()
	require.NoError(t, err)

	issuedAt := time.Now().Add(-23 * time.Hour).Truncate(time.Second)
	value, err := encryption.SignedValue(secret, HostSessionCookieName, []byte("ticket"), issuedAt)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app.example.com/", nil)
	req.AddCookie(&http.Cookie{Name: HostSessionCookieName, Value: value})

	req = h.withLegacyCookies(req)

	c, err := req.Cookie(SessionCookieName)
	require.NoError(t, err)
	_, ts, ok := encryption.Validate(c, secret, h.proxyConfig.Cookie.Expire)
	require.True(t, ok)
	assert.Equal(t, issuedAt, ts)
}
