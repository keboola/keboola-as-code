package preview

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	unknownKidRefetchInterval = time.Minute
	maxJWKSBodySize           = 64 << 10
	jwksFetchTimeout          = 10 * time.Second
)

type KeySetConfig struct {
	URL             string
	RefreshInterval time.Duration
	MaxStaleness    time.Duration
}

type KeySet struct {
	cfg    KeySetConfig
	client *http.Client
	clock  clockwork.Clock
	logger log.Logger

	lock        sync.Mutex
	keys        map[string]*ecdsa.PublicKey
	fetchedAt   time.Time
	lastAttempt time.Time
}

type jwksDocument struct {
	Keys []jwk `json:"keys"`
}

type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func NewKeySet(cfg KeySetConfig, clock clockwork.Clock, logger log.Logger) *KeySet {
	return &KeySet{
		cfg:    cfg,
		clock:  clock,
		logger: logger,
		keys:   make(map[string]*ecdsa.PublicKey),
		client: &http.Client{
			Timeout: jwksFetchTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (s *KeySet) Run(ctx context.Context) {
	s.refreshAndLog(ctx)
	ticker := s.clock.NewTicker(s.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			s.refreshAndLog(ctx)
		}
	}
}

func (s *KeySet) Refresh(ctx context.Context) error {
	s.markAttempt()
	keys, skipped, err := s.fetch(ctx)
	if err != nil {
		return err
	}
	for _, kid := range skipped {
		s.logger.Warnf(ctx, `preview: JWKS key "%s" skipped: only kty=EC, crv=P-256, use=sig, alg ES256 or absent, and 32-byte x/y are accepted`, SanitizeClaimForLog(kid))
	}
	s.store(keys)
	s.logger.Debugf(ctx, "preview: JWKS loaded, kids=%v", sortedKids(keys))
	return nil
}

func (s *KeySet) Key(ctx context.Context, kid string) (*ecdsa.PublicKey, error) {
	if key, ok := s.lookup(kid); ok {
		return key, nil
	}
	if !s.claimRefetch() {
		return nil, errors.Errorf(`preview: no usable key for kid "%s"`, SanitizeClaimForLog(kid))
	}
	s.refreshAndLog(context.WithoutCancel(ctx))
	if key, ok := s.lookup(kid); ok {
		return key, nil
	}
	return nil, errors.Errorf(`preview: no usable key for kid "%s"`, SanitizeClaimForLog(kid))
}

func (s *KeySet) refreshAndLog(ctx context.Context) {
	if err := s.Refresh(ctx); err != nil {
		s.logger.Warnf(ctx, "preview: JWKS refresh failed: %s", err)
	}
}

func (s *KeySet) markAttempt() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.lastAttempt = s.clock.Now()
}

func (s *KeySet) store(keys map[string]*ecdsa.PublicKey) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.keys = keys
	s.fetchedAt = s.clock.Now()
}

func (s *KeySet) lookup(kid string) (*ecdsa.PublicKey, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.fetchedAt.IsZero() || s.clock.Since(s.fetchedAt) > s.cfg.MaxStaleness {
		return nil, false
	}
	key, ok := s.keys[kid]
	return key, ok
}

func (s *KeySet) claimRefetch() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.lastAttempt.IsZero() && s.clock.Since(s.lastAttempt) < unknownKidRefetchInterval {
		return false
	}
	s.lastAttempt = s.clock.Now()
	return true
}

func (s *KeySet) fetch(ctx context.Context) (map[string]*ecdsa.PublicKey, []string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.URL, nil)
	if err != nil {
		return nil, nil, errors.Errorf("preview: JWKS request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, nil, errors.Errorf("preview: JWKS fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, errors.Errorf("preview: JWKS answered HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBodySize+1))
	if err != nil {
		return nil, nil, errors.Errorf("preview: JWKS read: %w", err)
	}
	if len(body) > maxJWKSBodySize {
		return nil, nil, errors.New("preview: JWKS response is too large")
	}
	return parseJWKS(body)
}

func parseJWKS(body []byte) (map[string]*ecdsa.PublicKey, []string, error) {
	var doc jwksDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, nil, errors.Errorf("preview: invalid JWKS: %w", err)
	}
	if doc.Keys == nil {
		return nil, nil, errors.New("preview: JWKS has no keys field")
	}
	keys := make(map[string]*ecdsa.PublicKey, len(doc.Keys))
	var skipped []string
	for _, k := range doc.Keys {
		pub, ok := parseJWK(k)
		if !ok {
			skipped = append(skipped, k.Kid)
			continue
		}
		keys[k.Kid] = pub
	}
	return keys, skipped, nil
}

func parseJWK(k jwk) (*ecdsa.PublicKey, bool) {
	if k.Kid == "" || k.Kty != "EC" || k.Crv != "P-256" || k.Use != "sig" {
		return nil, false
	}
	if k.Alg != "" && k.Alg != "ES256" {
		return nil, false
	}
	x, errX := base64.RawURLEncoding.DecodeString(k.X)
	y, errY := base64.RawURLEncoding.DecodeString(k.Y)
	if errX != nil || errY != nil || len(x) != 32 || len(y) != 32 {
		return nil, false
	}
	point := make([]byte, 0, 65)
	point = append(point, 4)
	point = append(point, x...)
	point = append(point, y...)
	pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
	if err != nil {
		return nil, false
	}
	return pub, true
}

func sortedKids(keys map[string]*ecdsa.PublicKey) []string {
	kids := make([]string, 0, len(keys))
	for kid := range keys {
		kids = append(kids, SanitizeClaimForLog(kid))
	}
	sort.Strings(kids)
	return kids
}
