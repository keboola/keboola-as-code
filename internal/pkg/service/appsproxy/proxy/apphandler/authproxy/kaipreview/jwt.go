// Package kaipreview implements the kai-preview iframe-auth path: a stateless
// JWT handshake (mint → bootstrap → exchange) that yields a host-only session
// cookie for embedding dev-mode data apps inside the kbc-ui SPA.
package kaipreview

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	purposeHandshake = "kai-preview-embed"
	purposeSession   = "kai-preview-session"
	handshakeTTL     = 60 * time.Second
)

// HandshakeClaims carries the authorization scope from mint to exchange.
type HandshakeClaims struct {
	AppID     string `json:"app_id"`
	ProjectID string `json:"project"`
	Purpose   string `json:"purpose"`
	JTI       string `json:"jti"`
	jwt.RegisteredClaims
}

// SessionClaims carries the authorization scope inside the session cookie.
type SessionClaims struct {
	AppID     string `json:"app_id"`
	ProjectID string `json:"project"`
	Purpose   string `json:"purpose"`
	TTL       int64  `json:"ttl_s"` // total intended lifetime in seconds (for halfway-refresh detection)
	jwt.RegisteredClaims
}

// NeedsRefresh returns true when the cookie has passed the midpoint of its TTL.
func (c SessionClaims) NeedsRefresh(now time.Time) bool {
	if c.IssuedAt == nil {
		return true
	}
	elapsed := now.Sub(c.IssuedAt.Time)
	return elapsed*2 > time.Duration(c.TTL)*time.Second
}

func MintHandshakeJWT(key string, clock clockwork.Clock, appID, projectID string) (string, error) {
	now := clock.Now()
	jti, err := randomHex(16)
	if err != nil {
		return "", errors.Errorf("kai-preview: generate jti: %w", err)
	}
	claims := HandshakeClaims{
		AppID:     appID,
		ProjectID: projectID,
		Purpose:   purposeHandshake,
		JTI:       jti,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(handshakeTTL)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(key))
	if err != nil {
		return "", errors.Errorf("kai-preview: sign handshake JWT: %w", err)
	}
	return signed, nil
}

func VerifyHandshakeJWT(key string, clock clockwork.Clock, raw string) (*HandshakeClaims, error) {
	claims := &HandshakeClaims{}
	parser := jwt.NewParser(jwt.WithTimeFunc(clock.Now), jwt.WithValidMethods([]string{"HS256"}))
	_, err := parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		return []byte(key), nil
	})
	if err != nil {
		return nil, errors.Errorf("kai-preview: verify handshake JWT: %w", err)
	}
	if claims.Purpose != purposeHandshake {
		return nil, errors.Errorf("kai-preview: handshake JWT has wrong purpose: %q", claims.Purpose)
	}
	return claims, nil
}

func MintSessionJWT(key string, clock clockwork.Clock, appID, projectID string, ttl time.Duration) (string, error) {
	now := clock.Now()
	claims := SessionClaims{
		AppID:     appID,
		ProjectID: projectID,
		Purpose:   purposeSession,
		TTL:       int64(ttl.Seconds()),
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(key))
	if err != nil {
		return "", errors.Errorf("kai-preview: sign session JWT: %w", err)
	}
	return signed, nil
}

func VerifySessionJWT(key string, clock clockwork.Clock, raw string) (*SessionClaims, error) {
	claims := &SessionClaims{}
	parser := jwt.NewParser(jwt.WithTimeFunc(clock.Now), jwt.WithValidMethods([]string{"HS256"}))
	_, err := parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		return []byte(key), nil
	})
	if err != nil {
		return nil, errors.Errorf("kai-preview: verify session JWT: %w", err)
	}
	if claims.Purpose != purposeSession {
		return nil, errors.Errorf("kai-preview: session JWT has wrong purpose: %q", claims.Purpose)
	}
	return claims, nil
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
