package preview

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/singleflight"

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

	snapshot    atomic.Pointer[keySnapshot]
	lastAttempt atomic.Pointer[time.Time]
	refetch     singleflight.Group
}

type keySnapshot struct {
	keys      map[string]*ecdsa.PublicKey
	fetchedAt time.Time
}

type jwksDocument struct {
	Keys *[]json.RawMessage `json:"keys"`
}

func NewKeySet(cfg KeySetConfig, clock clockwork.Clock, logger log.Logger) *KeySet {
	return &KeySet{
		cfg:    cfg,
		clock:  clock,
		logger: logger,
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
	<-s.refetch.DoChan("jwks", func() (any, error) {
		if s.claimRefetch() {
			s.refreshAndLog(context.WithoutCancel(ctx))
		}
		return nil, nil
	})
	if key, ok := s.lookup(kid); ok {
		return key, nil
	}
	return nil, errors.Errorf(`preview: no usable key for kid "%s"`, SanitizeClaimForLog(kid))
}

func (s *KeySet) refreshAndLog(ctx context.Context) {
	err := s.Refresh(ctx)
	if err == nil || ctx.Err() != nil {
		return
	}
	s.logger.Warnf(ctx, "preview: JWKS refresh failed: %s", err)
}

func (s *KeySet) markAttempt() {
	now := s.clock.Now()
	s.lastAttempt.Store(&now)
}

func (s *KeySet) store(keys map[string]*ecdsa.PublicKey) {
	s.snapshot.Store(&keySnapshot{keys: keys, fetchedAt: s.clock.Now()})
}

func (s *KeySet) lookup(kid string) (*ecdsa.PublicKey, bool) {
	snap := s.snapshot.Load()
	if snap == nil || s.clock.Since(snap.fetchedAt) > s.cfg.MaxStaleness {
		return nil, false
	}
	key, ok := snap.keys[kid]
	return key, ok
}

func (s *KeySet) claimRefetch() bool {
	now := s.clock.Now()
	last := s.lastAttempt.Load()
	if last != nil && now.Sub(*last) < unknownKidRefetchInterval {
		return false
	}
	return s.lastAttempt.CompareAndSwap(last, &now)
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
	keys := make(map[string]*ecdsa.PublicKey, len(*doc.Keys))
	var skipped []string
	for _, raw := range *doc.Keys {
		kid, pub, ok := parseJWK(raw)
		if !ok {
			skipped = append(skipped, kid)
			continue
		}
		keys[kid] = pub
	}
	return keys, skipped, nil
}

func parseJWK(raw json.RawMessage) (string, *ecdsa.PublicKey, bool) {
	var ident struct {
		Kid string `json:"kid"`
	}
	_ = json.Unmarshal(raw, &ident)

	var k jose.JSONWebKey
	if err := json.Unmarshal(raw, &k); err != nil {
		return ident.Kid, nil, false
	}
	if k.KeyID == "" || k.Use != "sig" || (k.Algorithm != "" && k.Algorithm != "ES256") {
		return ident.Kid, nil, false
	}
	pub, ok := k.Key.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return ident.Kid, nil, false
	}
	return k.KeyID, pub, true
}

func sortedKids(keys map[string]*ecdsa.PublicKey) []string {
	kids := make([]string, 0, len(keys))
	for kid := range keys {
		kids = append(kids, SanitizeClaimForLog(kid))
	}
	sort.Strings(kids)
	return kids
}
