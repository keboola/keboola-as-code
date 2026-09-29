package preview

import (
	"context"
	"crypto/ecdsa"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	LinkAudience = "apps-proxy"
	LinkPurpose  = "app-preview-link"
	ClockSkew    = 30 * time.Second
	linkVersion  = 1
	linkLifetime = 60 * time.Second
)

type KeyProvider interface {
	Key(ctx context.Context, kid string) (*ecdsa.PublicKey, error)
}

type LinkClaims struct {
	jwt.RegisteredClaims
	Purpose string `json:"purpose"`
	Ver     int    `json:"ver"`
	Kid     string `json:"-"`
}

type LinkVerifier struct {
	keys   KeyProvider
	issuer string
	clock  clockwork.Clock
}

func NewLinkVerifier(keys KeyProvider, issuer string, clock clockwork.Clock) *LinkVerifier {
	return &LinkVerifier{keys: keys, issuer: issuer, clock: clock}
}

func (v *LinkVerifier) Verify(ctx context.Context, raw, origin string) (*LinkClaims, error) {
	claims := &LinkClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodES256.Alg()}),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(LinkAudience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(ClockSkew),
		jwt.WithTimeFunc(v.clock.Now),
	)
	// The key is chosen by kid from the pinned JWKS only; jku, x5u and jwk headers are never read.
	_, err := parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		claims.Kid = kid
		if kid == "" {
			return nil, errors.New("missing kid")
		}
		return v.keys.Key(ctx, kid)
	})
	if err != nil {
		return claims, errors.Errorf("preview link rejected: %w", err)
	}
	return claims, checkLinkClaims(claims, origin)
}

func checkLinkClaims(c *LinkClaims, origin string) error {
	switch {
	case c.Purpose != LinkPurpose:
		return errors.New("preview link rejected: wrong purpose")
	case c.Ver != linkVersion:
		return errors.New("preview link rejected: unsupported ver")
	case c.ID == "":
		return errors.New("preview link rejected: missing jti")
	case c.IssuedAt == nil:
		return errors.New("preview link rejected: missing iat")
	case c.ExpiresAt.Sub(c.IssuedAt.Time) > linkLifetime+ClockSkew:
		return errors.New("preview link rejected: lifetime too long")
	}
	if c.Subject != origin {
		return errors.New("preview link rejected: sub does not match this host")
	}
	return nil
}
