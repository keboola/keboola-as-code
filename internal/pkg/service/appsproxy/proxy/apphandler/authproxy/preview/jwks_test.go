package preview_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
)

func newKeySet(t *testing.T, url string, clock clockwork.Clock) *preview.KeySet {
	t.Helper()
	return preview.NewKeySet(preview.KeySetConfig{
		URL:             url,
		RefreshInterval: 10 * time.Minute,
		MaxStaleness:    time.Hour,
	}, clock, log.NewNopLogger())
}

func withField(jwk map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(jwk))
	for k, v := range jwk {
		out[k] = v
	}
	if value == nil {
		delete(out, key)
		return out
	}
	out[key] = value
	return out
}

func TestKeySet_KeyFilter(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	good := previewtest.NewSigner(t, "good")
	noAlg := previewtest.NewSigner(t, "no-alg")
	base := previewtest.NewSigner(t, "x").JWK()
	zero := base64.RawURLEncoding.EncodeToString(make([]byte, 32))

	server := previewtest.NewJWKSServer(t,
		good.JWK(),
		withField(noAlg.JWK(), "alg", nil),
		withField(withField(base, "kid", "rsa"), "kty", "RSA"),
		withField(withField(base, "kid", "p384"), "crv", "P-384"),
		withField(withField(base, "kid", "enc"), "use", "enc"),
		withField(withField(base, "kid", "rs256"), "alg", "RS256"),
		withField(base, "kid", ""),
		withField(withField(withField(base, "kid", "off-curve"), "x", zero), "y", zero),
		withField(withField(base, "kid", "short"), "x", base64.RawURLEncoding.EncodeToString(make([]byte, 31))),
	)
	keys := newKeySet(t, server.JWKSURL(), clockwork.NewFakeClock())
	require.NoError(t, keys.Refresh(ctx))

	for _, kid := range []string{"good", "no-alg"} {
		_, err := keys.Key(ctx, kid)
		require.NoError(t, err, kid)
	}
	for _, kid := range []string{"rsa", "p384", "enc", "rs256", "", "off-curve", "short"} {
		_, err := keys.Key(ctx, kid)
		assert.Error(t, err, kid)
	}
}

func TestKeySet_FailedFetchKeepsLastGoodSet(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	signer := previewtest.NewSigner(t, "k1")
	server := previewtest.NewJWKSServer(t, signer.JWK())
	keys := newKeySet(t, server.JWKSURL(), clockwork.NewFakeClock())
	require.NoError(t, keys.Refresh(ctx))

	server.SetStatus(http.StatusInternalServerError)
	require.Error(t, keys.Refresh(ctx))
	server.SetStatus(http.StatusOK)
	server.SetBody([]byte("not json"))
	require.Error(t, keys.Refresh(ctx))
	server.SetBody([]byte(`{"keys":[` + strings.Repeat(`{"kty":"EC"},`, 8000) + `{}]}`))
	err := keys.Refresh(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too large")

	_, err = keys.Key(ctx, "k1")
	assert.NoError(t, err)
}

func TestKeySet_BodyWithoutKeysFieldKeepsLastGoodSet(t *testing.T) {
	t.Parallel()
	for _, body := range []string{`{}`, `null`, `{"error":"x"}`, `{"keys":null}`} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			signer := previewtest.NewSigner(t, "k1")
			server := previewtest.NewJWKSServer(t, signer.JWK())
			keys := newKeySet(t, server.JWKSURL(), clockwork.NewFakeClock())
			require.NoError(t, keys.Refresh(ctx))

			server.SetBody([]byte(body))
			require.Error(t, keys.Refresh(ctx))

			_, err := keys.Key(ctx, "k1")
			assert.NoError(t, err)
		})
	}
}

func TestKeySet_EmptySetRemovesKeys(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	signer := previewtest.NewSigner(t, "k1")
	server := previewtest.NewJWKSServer(t, signer.JWK())
	keys := newKeySet(t, server.JWKSURL(), clockwork.NewFakeClock())
	require.NoError(t, keys.Refresh(ctx))
	server.SetKeys()
	require.NoError(t, keys.Refresh(ctx))
	_, err := keys.Key(ctx, "k1")
	assert.Error(t, err)
}

func TestKeySet_DoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	signer := previewtest.NewSigner(t, "k1")
	other := previewtest.NewJWKSServer(t, signer.JWK())
	redirect := httptest.NewServer(http.RedirectHandler(other.JWKSURL(), http.StatusFound))
	t.Cleanup(redirect.Close)

	keys := newKeySet(t, redirect.URL, clockwork.NewFakeClock())
	require.Error(t, keys.Refresh(ctx))
	assert.Equal(t, int64(0), other.Hits())
}

func TestKeySet_StaleSetIsRejected(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	clock := clockwork.NewFakeClock()
	signer := previewtest.NewSigner(t, "k1")
	server := previewtest.NewJWKSServer(t, signer.JWK())
	keys := newKeySet(t, server.JWKSURL(), clock)
	require.NoError(t, keys.Refresh(ctx))
	server.SetStatus(http.StatusServiceUnavailable)

	clock.Advance(59 * time.Minute)
	_, err := keys.Key(ctx, "k1")
	require.NoError(t, err, "within maxStaleness the last good set is trusted")

	clock.Advance(2 * time.Minute)
	_, err = keys.Key(ctx, "k1")
	require.Error(t, err, "past maxStaleness the set is no longer trusted")

	server.SetStatus(http.StatusOK)
	clock.Advance(time.Minute)
	_, err = keys.Key(ctx, "k1")
	require.NoError(t, err, "a successful refetch makes the set fresh again")
}

func TestKeySet_UnknownKidRefetchAtMostOncePerMinute(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	clock := clockwork.NewFakeClock()
	k1 := previewtest.NewSigner(t, "k1")
	k2 := previewtest.NewSigner(t, "k2")
	server := previewtest.NewJWKSServer(t, k1.JWK())
	keys := newKeySet(t, server.JWKSURL(), clock)
	require.NoError(t, keys.Refresh(ctx))
	require.Equal(t, int64(1), server.Hits())

	server.SetKeys(k1.JWK(), k2.JWK())
	_, err := keys.Key(ctx, "k2")
	require.Error(t, err)
	assert.Equal(t, int64(1), server.Hits(), "no refetch within a minute of the last fetch")

	clock.Advance(61 * time.Second)
	_, err = keys.Key(ctx, "k2")
	require.NoError(t, err)
	assert.Equal(t, int64(2), server.Hits())

	_, err = keys.Key(ctx, "k3")
	require.Error(t, err)
	_, err = keys.Key(ctx, "k3")
	require.Error(t, err)
	assert.Equal(t, int64(2), server.Hits())
}

func TestKeySet_UnknownKidRefetchIgnoresRequestCancellation(t *testing.T) {
	t.Parallel()
	clock := clockwork.NewFakeClock()
	k1 := previewtest.NewSigner(t, "k1")
	k2 := previewtest.NewSigner(t, "k2")
	server := previewtest.NewJWKSServer(t, k1.JWK())
	keys := newKeySet(t, server.JWKSURL(), clock)
	require.NoError(t, keys.Refresh(t.Context()))

	server.SetKeys(k1.JWK(), k2.JWK())
	clock.Advance(61 * time.Second)
	cancelled, cancel := context.WithCancelCause(t.Context())
	cancel(nil)

	_, err := keys.Key(cancelled, "k2")
	require.NoError(t, err)
	assert.Equal(t, int64(2), server.Hits())
}

func TestKeySet_RunDoesNotWarnWhenCancelledDuringFetch(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	logger := log.NewDebugLogger()
	keys := preview.NewKeySet(preview.KeySetConfig{
		URL:             server.URL,
		RefreshInterval: 10 * time.Minute,
		MaxStaleness:    time.Hour,
	}, clockwork.NewFakeClock(), logger)

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	done := make(chan struct{})
	go func() {
		keys.Run(ctx)
		close(done)
	}()
	<-started
	cancel(nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on context cancellation")
	}
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func TestKeySet_RunWarnsWhenRefreshFails(t *testing.T) {
	t.Parallel()
	signer := previewtest.NewSigner(t, "k1")
	server := previewtest.NewJWKSServer(t, signer.JWK())
	server.SetStatus(http.StatusServiceUnavailable)
	logger := log.NewDebugLogger()
	keys := preview.NewKeySet(preview.KeySetConfig{
		URL:             server.JWKSURL(),
		RefreshInterval: 10 * time.Minute,
		MaxStaleness:    time.Hour,
	}, clockwork.NewFakeClock(), logger)

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	go keys.Run(ctx)
	require.Eventually(t, func() bool {
		return strings.Contains(logger.WarnMessages(), "JWKS refresh failed")
	}, 5*time.Second, 10*time.Millisecond)
}

func TestKeySet_RunSurvivesDownJWKS(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	clock := clockwork.NewFakeClock()
	signer := previewtest.NewSigner(t, "k1")
	server := previewtest.NewJWKSServer(t, signer.JWK())
	server.SetStatus(http.StatusServiceUnavailable)
	keys := newKeySet(t, server.JWKSURL(), clock)

	done := make(chan struct{})
	go func() {
		keys.Run(ctx)
		close(done)
	}()
	require.Eventually(t, func() bool { return server.Hits() >= 1 }, 5*time.Second, 10*time.Millisecond)
	_, err := keys.Key(ctx, "k1")
	require.Error(t, err)

	server.SetStatus(http.StatusOK)
	require.NoError(t, clock.BlockUntilContext(ctx, 1))
	clock.Advance(10 * time.Minute)
	require.Eventually(t, func() bool {
		_, err := keys.Key(ctx, "k1")
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	cancel(nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on context cancellation")
	}
}
