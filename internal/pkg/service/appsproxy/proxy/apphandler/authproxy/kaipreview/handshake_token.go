package kaipreview

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// StorageTokenVerifier abstracts HTTPStorageTokenVerifier so tests can inject a stub without HTTP.
type StorageTokenVerifier interface {
	Verify(ctx context.Context, token string) (*StorageTokenVerifyResult, error)
}

// DevModeChecker tells the handler whether the current app is in dev mode.
// Backed by the apps-proxy CRD watcher (AppInfo.DevMode).
type DevModeChecker interface {
	IsDevMode(ctx context.Context, appID string) bool
}

type HandshakeTokenDeps struct {
	Clock                clockwork.Clock
	StorageTokenVerifier StorageTokenVerifier
	DevMode              DevModeChecker
	CORS                 *CORS
	HandshakeKey         string
	AppID                string
	AppProjectID         string
}

type HandshakeTokenHandler struct {
	deps HandshakeTokenDeps
}

func NewHandshakeTokenHandler(deps HandshakeTokenDeps) *HandshakeTokenHandler {
	return &HandshakeTokenHandler{deps: deps}
}

func (h *HandshakeTokenHandler) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error {
	if h.deps.CORS.HandlePreflight(w, r) {
		return nil
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil
	}

	// Origin must be allowed even on the actual request (defence in depth).
	origin := r.Header.Get("Origin")
	if !h.deps.CORS.IsAllowed(origin) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return nil
	}
	// All remaining responses are to an allowed origin — emit CORS headers now so the
	// SPA can read status codes and bodies of auth-failure responses, not just successes.
	h.deps.CORS.WriteResponseHeaders(w, origin)

	// Dev-mode gate first: pretend the endpoint doesn't exist on non-dev apps.
	if !h.deps.DevMode.IsDevMode(r.Context(), h.deps.AppID) {
		http.NotFound(w, r)
		return nil
	}

	storageToken := r.Header.Get("X-StorageApi-Token")
	if storageToken == "" {
		http.Error(w, "missing X-StorageApi-Token", http.StatusUnauthorized)
		return nil
	}

	res, err := h.deps.StorageTokenVerifier.Verify(r.Context(), storageToken)
	if err != nil {
		// Never echo the raw Storage token in the error body or logs.
		http.Error(w, "Storage token invalid", http.StatusUnauthorized)
		return nil
	}
	if res.ProjectID != h.deps.AppProjectID {
		http.Error(w, "app belongs to a different project", http.StatusForbidden)
		return nil
	}

	jwt, err := MintHandshakeJWT(h.deps.HandshakeKey, h.deps.Clock, h.deps.AppID, h.deps.AppProjectID)
	if err != nil {
		return errors.Errorf("kai-preview: mint handshake JWT: %w", err)
	}

	// CORS headers already set above.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"token": jwt})
	return nil
}
