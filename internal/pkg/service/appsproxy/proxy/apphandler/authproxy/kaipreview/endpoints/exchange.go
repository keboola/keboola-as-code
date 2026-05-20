package endpoints

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ExchangeDeps struct {
	Logger       log.Logger
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
		h.deps.Logger.With(attribute.String("appID", h.deps.AppID)).
			Warn(r.Context(), "kai-preview: exchange requested on non-dev-mode app")
		http.NotFound(w, r)
		return nil
	}

	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return nil //nolint:nilerr // intentional: decode error handled via HTTP 400 response
	}

	claims, err := kaipreview.VerifyHandshakeJWT(h.deps.HandshakeKey, h.deps.Clock, body.Token)
	if err != nil {
		h.deps.Logger.With(
			attribute.String("appID", h.deps.AppID),
			attribute.String("error", err.Error()),
		).Warn(r.Context(), "kai-preview: handshake JWT verify failed")
		http.Error(w, "invalid handshake token", http.StatusUnauthorized)
		return nil //nolint:nilerr // intentional: invalid token handled via HTTP 401 response
	}
	if claims.AppID != h.deps.AppID || claims.ProjectID != h.deps.AppProjectID {
		h.deps.Logger.With(
			attribute.String("appID", h.deps.AppID),
			attribute.String("jwtAppID", claims.AppID),
		).Warn(r.Context(), "kai-preview: handshake JWT scope mismatch")
		http.Error(w, "handshake token scope mismatch", http.StatusForbidden)
		return nil
	}

	sessionJWT, err := kaipreview.MintSessionJWT(h.deps.SessionKey, h.deps.Clock, h.deps.AppID, h.deps.AppProjectID, h.deps.SessionTTL)
	if err != nil {
		return errors.Errorf("kai-preview: mint session JWT: %w", err)
	}

	h.deps.Logger.With(
		attribute.String("appID", h.deps.AppID),
		attribute.String("projectID", h.deps.AppProjectID),
		attribute.String("jti", claims.ID),
	).Info(r.Context(), "kai-preview: exchanged handshake JWT for session cookie")

	kaipreview.SetSessionCookie(w, sessionJWT, h.deps.SessionTTL)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	return nil
}
