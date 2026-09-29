// Package previewtest provides an ES256 link signer and a fake JWKS endpoint for preview tests.
package previewtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

const Issuer = "https://apps.keboola.local"

type Signer struct {
	Kid string
	Key *ecdsa.PrivateKey
}

func NewSigner(tb testing.TB, kid string) *Signer {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(tb, err)
	return &Signer{Kid: kid, Key: key}
}

func (s *Signer) JWK() map[string]any {
	raw, err := s.Key.PublicKey.Bytes()
	if err != nil {
		panic(err)
	}
	return map[string]any{
		"kty": "EC",
		"crv": "P-256",
		"use": "sig",
		"alg": "ES256",
		"kid": s.Kid,
		"x":   base64.RawURLEncoding.EncodeToString(raw[1:33]),
		"y":   base64.RawURLEncoding.EncodeToString(raw[33:65]),
	}
}

type Claims struct {
	Issuer        string
	Audience      any
	Subject       string
	Purpose       string
	Ver           int
	ID            string
	IssuedAt      time.Time
	ExpiresAt     time.Time
	OmitIssuedAt  bool
	OmitExpiresAt bool
	ExtraHeaders  map[string]any
}

func ValidClaims(now time.Time, sub string) Claims {
	return Claims{
		Issuer:    Issuer,
		Audience:  "apps-proxy",
		Subject:   sub,
		Purpose:   "app-preview-link",
		Ver:       1,
		ID:        "0123456789abcdef0123456789abcdef",
		IssuedAt:  now,
		ExpiresAt: now.Add(60 * time.Second),
	}
}

func (c Claims) Map() jwt.MapClaims {
	m := jwt.MapClaims{
		"iss":     c.Issuer,
		"aud":     c.Audience,
		"sub":     c.Subject,
		"purpose": c.Purpose,
		"ver":     c.Ver,
		"jti":     c.ID,
	}
	if !c.OmitIssuedAt {
		m["iat"] = c.IssuedAt.Unix()
	}
	if !c.OmitExpiresAt {
		m["exp"] = c.ExpiresAt.Unix()
	}
	return m
}

func (s *Signer) Mint(tb testing.TB, c Claims) string {
	tb.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodES256, c.Map())
	token.Header["kid"] = s.Kid
	for k, v := range c.ExtraHeaders {
		token.Header[k] = v
	}
	signed, err := token.SignedString(s.Key)
	require.NoError(tb, err)
	return signed
}

type JWKSServer struct {
	*httptest.Server
	lock   sync.Mutex
	body   []byte
	status int
	hits   atomic.Int64
}

func NewJWKSServer(tb testing.TB, keys ...map[string]any) *JWKSServer {
	tb.Helper()
	s := &JWKSServer{status: http.StatusOK}
	s.SetKeys(keys...)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.hits.Add(1)
		s.lock.Lock()
		defer s.lock.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(s.status)
		_, _ = w.Write(s.body)
	}))
	tb.Cleanup(s.Close)
	return s
}

func (s *JWKSServer) SetKeys(keys ...map[string]any) {
	if keys == nil {
		keys = []map[string]any{}
	}
	body, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		panic(err)
	}
	s.SetBody(body)
}

func (s *JWKSServer) SetBody(body []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.body = body
}

func (s *JWKSServer) SetStatus(code int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.status = code
}

func (s *JWKSServer) Hits() int64 {
	return s.hits.Load()
}

func (s *JWKSServer) JWKSURL() string {
	return s.URL + "/.well-known/jwks.json"
}
