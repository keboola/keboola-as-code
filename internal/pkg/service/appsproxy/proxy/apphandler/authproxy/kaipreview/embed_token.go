package kaipreview

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// STATokenVerifier abstracts STAVerifier so tests can inject a stub without HTTP.
type STATokenVerifier interface {
	Verify(ctx context.Context, token string) (*STAVerifyResult, error)
}

// DevModeChecker tells the handler whether the current app is in dev mode.
// Backed by the apps-proxy CRD watcher (AppInfo.DevMode).
type DevModeChecker interface {
	IsDevMode(appID string) bool
}

type EmbedTokenDeps struct {
	Clock        clockwork.Clock
	STA          STATokenVerifier
	DevMode      DevModeChecker
	CORS         *CORS
	HandshakeKey string
	AppID        string
	AppProjectID string
}

type EmbedTokenHandler struct {
	deps EmbedTokenDeps
}

func NewEmbedTokenHandler(deps EmbedTokenDeps) *EmbedTokenHandler {
	return &EmbedTokenHandler{deps: deps}
}

func (h *EmbedTokenHandler) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error {
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
	if !h.deps.DevMode.IsDevMode(h.deps.AppID) {
		http.NotFound(w, r)
		return nil
	}

	staToken := r.Header.Get("X-StorageApi-Token")
	if staToken == "" {
		http.Error(w, "missing X-StorageApi-Token", http.StatusUnauthorized)
		return nil
	}

	res, err := h.deps.STA.Verify(r.Context(), staToken)
	if err != nil {
		// Never echo the raw STA token in the error body or logs.
		http.Error(w, "STA token invalid", http.StatusUnauthorized)
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
