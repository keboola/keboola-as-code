package endpoints

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// DevModeChecker tells the handler whether the current app is in dev mode.
// Backed by the apps-proxy CRD watcher (AppInfo.DevMode).
type DevModeChecker interface {
	IsDevMode(ctx context.Context, appID string) bool
}

type HandshakeTokenDeps struct {
	Logger               log.Logger
	Clock                clockwork.Clock
	StorageTokenVerifier kaipreview.StorageTokenVerifier
	DevMode              DevModeChecker
	CORS                 *kaipreview.CORS
	HandshakeKey         string
	SessionTTL           time.Duration
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
	// Mint authenticates by X-StorageApi-Token header, not by cookie — no ACAC on preflight.
	if h.deps.CORS.HandlePreflight(w, r, false) {
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
	// Mint uses header auth; no credentials header needed on the actual response either.
	h.deps.CORS.WriteResponseHeaders(w, origin, false)

	// Dev-mode gate first: pretend the endpoint doesn't exist on non-dev apps.
	if !h.deps.DevMode.IsDevMode(r.Context(), h.deps.AppID) {
		h.deps.Logger.With(attribute.String("appID", h.deps.AppID)).
			Warn(r.Context(), "kai-preview: handshake-token requested on non-dev-mode app")
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
		h.deps.Logger.With(
			attribute.String("appID", h.deps.AppID),
			attribute.String("error", err.Error()),
		).Warn(r.Context(), "kai-preview: Storage token verify failed")
		http.Error(w, "Storage token invalid", http.StatusUnauthorized)
		return nil //nolint:nilerr // intentional: error is handled via HTTP response, not propagated
	}
	if res.ProjectID != h.deps.AppProjectID {
		h.deps.Logger.With(
			attribute.String("appID", h.deps.AppID),
			attribute.String("appProjectID", h.deps.AppProjectID),
			attribute.String("tokenProjectID", res.ProjectID),
		).Warn(r.Context(), "kai-preview: Storage token's project does not match app")
		http.Error(w, "app belongs to a different project", http.StatusForbidden)
		return nil
	}

	mintedJWT, err := kaipreview.MintHandshakeJWT(h.deps.HandshakeKey, h.deps.Clock, h.deps.AppID, h.deps.AppProjectID)
	if err != nil {
		return errors.Errorf("kai-preview: mint handshake JWT: %w", err)
	}

	// Extract jti from the minted token for logging and response (non-sensitive).
	var jti string
	if claims, parseErr := kaipreview.VerifyHandshakeJWT(h.deps.HandshakeKey, h.deps.Clock, mintedJWT); parseErr == nil {
		jti = claims.ID
		h.deps.Logger.With(
			attribute.String("appID", h.deps.AppID),
			attribute.String("projectID", h.deps.AppProjectID),
			attribute.String("jti", jti),
		).Info(r.Context(), "kai-preview: minted handshake JWT")
	}

	// CORS headers already set above.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	// Response shape: {token, jti, sessionTtlSeconds}.
	// The SPA uses sessionTtlSeconds to derive its heartbeat cadence (divide by 2).
	// jti is included so the SPA can correlate logs across the handshake.
	resp := struct {
		Token             string `json:"token"`
		JTI               string `json:"jti"`
		SessionTTLSeconds int    `json:"sessionTtlSeconds"`
	}{
		Token:             mintedJWT,
		JTI:               jti,
		SessionTTLSeconds: int(h.deps.SessionTTL.Seconds()),
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return errors.Errorf("kai-preview: write handshake token response: %w", err)
	}
	return nil
}

