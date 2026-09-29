package preview

import (
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	SessionCookieName = "__Host-kbc-app-preview-session"
	sessionPurpose    = "app-preview-session"
	sessionVersion    = 1
	sessionIDLength   = 32
)

type SessionClaims struct {
	jwt.RegisteredClaims
	Ver      int    `json:"ver"`
	Purpose  string `json:"purpose"`
	AuthTime int64  `json:"authTime"`
	LinkJTI  string `json:"linkJti"`
}

type Sessions struct {
	key    []byte
	idle   time.Duration
	maxTTL time.Duration
	clock  clockwork.Clock
}

func NewSessions(key string, idle, maxTTL time.Duration, clock clockwork.Clock) *Sessions {
	return &Sessions{key: []byte(key), idle: idle, maxTTL: maxTTL, clock: clock}
}

func (s *Sessions) Issue(origin, linkJTI string) (*http.Cookie, error) {
	now := s.clock.Now()
	return s.cookie(origin, now, now.Unix(), linkJTI)
}

func (s *Sessions) Check(raw, origin string) (*SessionClaims, *http.Cookie, bool) {
	if raw == "" {
		return nil, nil, false
	}
	now := s.clock.Now()
	claims := &SessionClaims{}
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

func (s *Sessions) valid(c *SessionClaims, now time.Time) bool {
	return c.Purpose == sessionPurpose &&
		c.Ver == sessionVersion &&
		c.AuthTime > 0 &&
		c.IssuedAt != nil &&
		now.Before(time.Unix(c.AuthTime, 0).Add(s.maxTTL))
}

func (s *Sessions) shouldSlide(c *SessionClaims, now time.Time) bool {
	return now.Sub(c.IssuedAt.Time) >= SessionSlideInterval && s.expiry(now, c.AuthTime).After(c.ExpiresAt.Time)
}

func (s *Sessions) expiry(now time.Time, authTime int64) time.Time {
	exp := now.Add(s.idle)
	if hardCap := time.Unix(authTime, 0).Add(s.maxTTL); hardCap.Before(exp) {
		exp = hardCap
	}
	return exp.Truncate(time.Second)
}

func (s *Sessions) cookie(origin string, now time.Time, authTime int64, linkJTI string) (*http.Cookie, error) {
	exp := s.expiry(now, authTime)
	claims := SessionClaims{
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
		Name:        SessionCookieName,
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

func ClearSessionCookie() *http.Cookie {
	return &http.Cookie{
		Name:        SessionCookieName,
		Path:        "/",
		MaxAge:      -1,
		Secure:      true,
		HttpOnly:    true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
	}
}

func TakeSessionCookie(req *http.Request) string {
	value := ""
	if c, err := req.Cookie(SessionCookieName); err == nil {
		value = c.Value
	}
	lines := req.Header.Values("Cookie")
	if len(lines) == 0 {
		return value
	}
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if l := withoutCookie(line, SessionCookieName); l != "" {
			kept = append(kept, l)
		}
	}
	req.Header.Del("Cookie")
	for _, l := range kept {
		req.Header.Add("Cookie", l)
	}
	return value
}

func withoutCookie(line, name string) string {
	parts := strings.Split(line, ";")
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		cookieName, _, _ := strings.Cut(p, "=")
		if p == "" || strings.TrimSpace(cookieName) == name {
			continue
		}
		kept = append(kept, p)
	}
	return strings.Join(kept, "; ")
}
