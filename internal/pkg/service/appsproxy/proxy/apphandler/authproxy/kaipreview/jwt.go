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
	purposeHandshake = "kai-preview-handshake"
	purposeSession   = "kai-preview-session"
	handshakeTTL     = 60 * time.Second
)

// HandshakeClaims carries the authorization scope from mint to exchange.
type HandshakeClaims struct {
	// Ver is the JWT schema version. Always 1 for this iteration.
	// A version field lets old and new proxies coexist during rolling deploys
	// when the claim shape changes in a future release.
	Ver       int    `json:"ver"`
	AppID     string `json:"appId"`
	ProjectID string `json:"project"`
	Purpose   string `json:"purpose"`
	// Reserved for future identity propagation per the dev-iframe-auth design spec
	// ("Possible future extensions"). Not populated today; omitempty ensures
	// wire format is unchanged until a consumer is ready.
	Email string   `json:"email,omitempty"`
	Name  string   `json:"name,omitempty"`
	Roles []string `json:"roles,omitempty"`
	jwt.RegisteredClaims
}

// SessionClaims carries the authorization scope inside the session cookie.
type SessionClaims struct {
	// Ver is the JWT schema version. Always 1 for this iteration.
	// A version field lets old and new proxies coexist during rolling deploys
	// when the claim shape changes in a future release.
	Ver       int    `json:"ver"`
	AppID     string `json:"appId"`
	ProjectID string `json:"project"`
	Purpose   string `json:"purpose"`
	TTL       int64  `json:"ttlS"` // total intended lifetime in seconds (for halfway-refresh detection)
	// Reserved for future identity propagation per the dev-iframe-auth design spec
	// ("Possible future extensions"). Not populated today; omitempty ensures
	// wire format is unchanged until a consumer is ready.
	Email string   `json:"email,omitempty"`
	Name  string   `json:"name,omitempty"`
	Roles []string `json:"roles,omitempty"`
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
		Ver:       1,
		AppID:     appID,
		ProjectID: projectID,
		Purpose:   purposeHandshake,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        jti,
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
	jti, err := randomHex(16)
	if err != nil {
		return "", errors.Errorf("kai-preview: generate jti: %w", err)
	}
	claims := SessionClaims{
		Ver:       1,
		AppID:     appID,
		ProjectID: projectID,
		Purpose:   purposeSession,
		TTL:       int64(ttl.Seconds()),
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        jti,
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
