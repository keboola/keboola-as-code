package kaipreview

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ExchangeDeps struct {
	Clock        clockwork.Clock
	DevMode      DevModeChecker
	HandshakeKey string
	SessionKey   string
	SessionTTL   time.Duration
	AppID        string
	AppProjectID string
}

type ExchangeHandler struct {
	deps ExchangeDeps
}

func NewExchangeHandler(deps ExchangeDeps) *ExchangeHandler {
	return &ExchangeHandler{deps: deps}
}

func (h *ExchangeHandler) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil
	}

	// Dev-mode gate: pretend the endpoint doesn't exist on non-dev apps.
	if !h.deps.DevMode.IsDevMode(r.Context(), h.deps.AppID) {
		http.NotFound(w, r)
		return nil
	}

	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return nil
	}

	claims, err := VerifyHandshakeJWT(h.deps.HandshakeKey, h.deps.Clock, body.Token)
	if err != nil {
		http.Error(w, "invalid handshake token", http.StatusUnauthorized)
		return nil
	}
	if claims.AppID != h.deps.AppID || claims.ProjectID != h.deps.AppProjectID {
		http.Error(w, "handshake token scope mismatch", http.StatusForbidden)
		return nil
	}

	sessionJWT, err := MintSessionJWT(h.deps.SessionKey, h.deps.Clock, h.deps.AppID, h.deps.AppProjectID, h.deps.SessionTTL)
	if err != nil {
		return errors.Errorf("kai-preview: mint session JWT: %w", err)
	}

	SetSessionCookie(w, sessionJWT, h.deps.SessionTTL)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	return nil
}
