package preview_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/previewtest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const testOrigin = "https://my-app-123.hub.keboola.local"

type staticKeys map[string]*ecdsa.PublicKey

func (s staticKeys) Key(_ context.Context, kid string) (*ecdsa.PublicKey, error) {
	if k, ok := s[kid]; ok {
		return k, nil
	}
	return nil, errors.New("unknown kid")
}

func TestLinkVerifier(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	signer := previewtest.NewSigner(t, "2026-09")
	stranger := previewtest.NewSigner(t, "2026-09")
	verifier := preview.NewLinkVerifier(staticKeys{"2026-09": &signer.Key.PublicKey}, previewtest.Issuer, clockwork.NewFakeClockAt(now))

	cases := []struct {
		name   string
		mutate func(c *previewtest.Claims)
		signer *previewtest.Signer
		ok     bool
	}{
		{name: "valid", mutate: func(*previewtest.Claims) {}, ok: true},
		{name: "aud-array-containing-apps-proxy", mutate: func(c *previewtest.Claims) { c.Audience = []string{"apps-proxy"} }, ok: true},
		{name: "wrong-aud", mutate: func(c *previewtest.Claims) { c.Audience = "sandboxes-service" }},
		{name: "wrong-iss", mutate: func(c *previewtest.Claims) { c.Issuer = "https://apps.evil.local" }},
		{name: "sub-other-app", mutate: func(c *previewtest.Claims) { c.Subject = "https://other-999.hub.keboola.local" }},
		{name: "sub-upper-case-trailing-slash-443", mutate: func(c *previewtest.Claims) { c.Subject = "HTTPS://MY-APP-123.HUB.KEBOOLA.LOCAL:443/" }},
		{name: "sub-upper-case", mutate: func(c *previewtest.Claims) { c.Subject = "HTTPS://MY-APP-123.HUB.KEBOOLA.LOCAL" }},
		{name: "sub-upper-case-host", mutate: func(c *previewtest.Claims) { c.Subject = "https://MY-APP-123.hub.keboola.local" }},
		{name: "sub-default-port", mutate: func(c *previewtest.Claims) { c.Subject = testOrigin + ":443" }},
		{name: "sub-trailing-slash", mutate: func(c *previewtest.Claims) { c.Subject = testOrigin + "/" }},
		{name: "sub-with-path", mutate: func(c *previewtest.Claims) { c.Subject = testOrigin + "/x" }},
		{name: "sub-http", mutate: func(c *previewtest.Claims) { c.Subject = "http://my-app-123.hub.keboola.local" }},
		{name: "sub-other-port", mutate: func(c *previewtest.Claims) { c.Subject = testOrigin + ":8443" }},
		{name: "wrong-purpose", mutate: func(c *previewtest.Claims) { c.Purpose = "app-preview-session" }},
		{name: "ver-2", mutate: func(c *previewtest.Claims) { c.Ver = 2 }},
		{name: "missing-jti", mutate: func(c *previewtest.Claims) { c.ID = "" }},
		{name: "missing-exp", mutate: func(c *previewtest.Claims) { c.OmitExpiresAt = true }},
		{name: "missing-iat", mutate: func(c *previewtest.Claims) { c.OmitIssuedAt = true }},
		{name: "expired-beyond-skew", mutate: func(c *previewtest.Claims) {
			c.IssuedAt = now.Add(-100 * time.Second)
			c.ExpiresAt = now.Add(-31 * time.Second)
		}},
		{name: "iat-25s-in-future-accepted", mutate: func(c *previewtest.Claims) {
			c.IssuedAt = now.Add(25 * time.Second)
			c.ExpiresAt = c.IssuedAt.Add(60 * time.Second)
		}, ok: true},
		{name: "iat-45s-in-future-rejected", mutate: func(c *previewtest.Claims) {
			c.IssuedAt = now.Add(45 * time.Second)
			c.ExpiresAt = c.IssuedAt.Add(60 * time.Second)
		}},
		{name: "lifetime-90s-accepted", mutate: func(c *previewtest.Claims) { c.ExpiresAt = now.Add(90 * time.Second) }, ok: true},
		{name: "lifetime-91s-rejected", mutate: func(c *previewtest.Claims) { c.ExpiresAt = now.Add(91 * time.Second) }},
		{name: "unknown-kid", mutate: func(*previewtest.Claims) {}, signer: &previewtest.Signer{Kid: "other", Key: signer.Key}},
		{name: "missing-kid", mutate: func(*previewtest.Claims) {}, signer: &previewtest.Signer{Kid: "", Key: signer.Key}},
		{name: "known-kid-foreign-key", mutate: func(*previewtest.Claims) {}, signer: stranger},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			claims := previewtest.ValidClaims(now, testOrigin)
			tc.mutate(&claims)
			s := signer
			if tc.signer != nil {
				s = tc.signer
			}
			got, err := verifier.Verify(t.Context(), s.Mint(t, claims), testOrigin)
			require.NotNil(t, got)
			if tc.ok {
				require.NoError(t, err)
				assert.Equal(t, claims.ID, got.ID)
				assert.Equal(t, s.Kid, got.Kid)
				return
			}
			require.Error(t, err)
		})
	}
}

func TestLinkVerifier_RejectsOtherAlgorithms(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	signer := previewtest.NewSigner(t, "2026-09")
	verifier := preview.NewLinkVerifier(staticKeys{"2026-09": &signer.Key.PublicKey}, previewtest.Issuer, clockwork.NewFakeClockAt(now))
	claims := previewtest.ValidClaims(now, testOrigin).Map()

	pubBytes, err := signer.Key.PublicKey.Bytes()
	require.NoError(t, err)
	hs := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	hs.Header["kid"] = "2026-09"
	hsToken, err := hs.SignedString(pubBytes)
	require.NoError(t, err)

	none := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	none.Header["kid"] = "2026-09"
	noneToken, err := none.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)
	es384 := jwt.NewWithClaims(jwt.SigningMethodES384, claims)
	es384.Header["kid"] = "2026-09"
	es384Token, err := es384.SignedString(p384)
	require.NoError(t, err)

	for name, token := range map[string]string{"HS256": hsToken, "none": noneToken, "ES384": es384Token, "garbage": "a.b.c"} {
		_, err := verifier.Verify(t.Context(), token, testOrigin)
		assert.Error(t, err, name)
	}
}

func TestLinkVerifier_IgnoresKeyHeaders(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	signer := previewtest.NewSigner(t, "2026-09")
	attacker := previewtest.NewSigner(t, "attacker")
	verifier := preview.NewLinkVerifier(staticKeys{"2026-09": &signer.Key.PublicKey}, previewtest.Issuer, clockwork.NewFakeClockAt(now))

	var hits atomic.Int64
	attackerJWKS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(attackerJWKS.Close)

	headers := map[string]any{"jku": attackerJWKS.URL, "x5u": attackerJWKS.URL, "jwk": attacker.JWK()}

	forged := previewtest.ValidClaims(now, testOrigin)
	forged.ExtraHeaders = headers
	_, err := verifier.Verify(t.Context(), attacker.Mint(t, forged), testOrigin)
	require.Error(t, err, "a key named in the token must never be used")

	legit := previewtest.ValidClaims(now, testOrigin)
	legit.ExtraHeaders = headers
	_, err = verifier.Verify(t.Context(), signer.Mint(t, legit), testOrigin)
	require.NoError(t, err, "the headers are ignored, not treated as an error")

	assert.Equal(t, int64(0), hits.Load(), "jku/x5u are never fetched")
}
