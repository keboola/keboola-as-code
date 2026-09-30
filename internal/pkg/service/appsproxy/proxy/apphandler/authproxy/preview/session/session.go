package session

import (
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	IdleTTL         = 4 * time.Hour
	MaxTTL          = 12 * time.Hour
	SlideInterval   = 5 * time.Minute
	sessionPurpose  = "app-preview-session"
	sessionVersion  = 1
	sessionIDLength = 32
)

type Claims struct {
	jwt.RegisteredClaims
	Ver      int    `json:"ver"`
	Purpose  string `json:"purpose"`
	AuthTime int64  `json:"authTime"`
	LinkJTI  string `json:"linkJti"`
}

type Manager struct {
	key    []byte
	idle   time.Duration
	maxTTL time.Duration
	clock  clockwork.Clock
}

func NewManager(key string, idle, maxTTL time.Duration, clock clockwork.Clock) *Manager {
	return &Manager{key: []byte(key), idle: idle, maxTTL: maxTTL, clock: clock}
}

func (s *Manager) Issue(origin, linkJTI string) (*http.Cookie, error) {
	now := s.clock.Now()
	return s.cookie(origin, now, now.Unix(), linkJTI)
}

func (s *Manager) Check(raw, origin string) (*Claims, *http.Cookie, bool) {
	if raw == "" {
		return nil, nil, false
	}
	now := s.clock.Now()
	claims := &Claims{}
	_, err := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithSubject(origin),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(s.clock.Now),
	).ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return s.key, nil })
	if err != nil || !s.valid(claims, now) {
		return nil, nil, false
	}
	if !s.shouldSlide(claims, now) {
		return claims, nil, true
	}
	refresh, err := s.cookie(origin, now, claims.AuthTime, claims.LinkJTI)
	if err != nil {
		return claims, nil, true
	}
	return claims, refresh, true
}

func (s *Manager) valid(c *Claims, now time.Time) bool {
	return c.Purpose == sessionPurpose &&
		c.Ver == sessionVersion &&
		c.AuthTime > 0 &&
		c.IssuedAt != nil &&
		now.Before(time.Unix(c.AuthTime, 0).Add(s.maxTTL))
}

func (s *Manager) shouldSlide(c *Claims, now time.Time) bool {
	return now.Sub(c.IssuedAt.Time) >= SlideInterval && s.expiry(now, c.AuthTime).After(c.ExpiresAt.Time)
}

func (s *Manager) expiry(now time.Time, authTime int64) time.Time {
	exp := now.Add(s.idle)
	if hardCap := time.Unix(authTime, 0).Add(s.maxTTL); hardCap.Before(exp) {
		exp = hardCap
	}
	return exp.Truncate(time.Second)
}

func (s *Manager) cookie(origin string, now time.Time, authTime int64, linkJTI string) (*http.Cookie, error) {
	exp := s.expiry(now, authTime)
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   origin,
			ID:        idgenerator.Random(sessionIDLength),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
		Ver:      sessionVersion,
		Purpose:  sessionPurpose,
		AuthTime: authTime,
		LinkJTI:  linkJTI,
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.key)
	if err != nil {
		return nil, errors.Errorf("preview: sign session: %w", err)
	}
	return &http.Cookie{
		Name:        CookieName,
		Value:       signed,
		Path:        "/",
		Expires:     exp,
		MaxAge:      max(1, int(exp.Unix()-now.Unix())),
		Secure:      true,
		HttpOnly:    true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
	}, nil
}
