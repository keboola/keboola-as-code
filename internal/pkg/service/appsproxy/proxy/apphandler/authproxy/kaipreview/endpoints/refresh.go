package endpoints

import (
	"net/http"
	"time"

	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type RefreshDeps struct {
	Logger       log.Logger
	Clock        clockwork.Clock
	DevMode      DevModeChecker
	SessionKey   string
	SessionTTL   time.Duration
	CORS         *kaipreview.CORS
	AppID        string
	AppProjectID string
}

type RefreshHandler struct {
	deps RefreshDeps
}

func NewRefreshHandler(deps RefreshDeps) *RefreshHandler {
	return &RefreshHandler{deps: deps}
}

func (h *RefreshHandler) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error {
	if h.deps.CORS.HandlePreflight(w, r) {
		return nil
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil
	}

	origin := r.Header.Get("Origin")
	if !h.deps.CORS.IsAllowed(origin) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return nil
	}
	// All remaining responses are to an allowed origin — emit CORS headers now so the
	// SPA can read status codes and bodies of auth-failure responses, not just successes.
	h.deps.CORS.WriteResponseHeaders(w, origin)

	if !h.deps.DevMode.IsDevMode(r.Context(), h.deps.AppID) {
		h.deps.Logger.With(attribute.String("appID", h.deps.AppID)).
			Warn(r.Context(), "kai-preview: refresh requested on non-dev-mode app")
		http.NotFound(w, r)
		return nil
	}

	cookieValue := kaipreview.ReadSessionCookie(r)
	if cookieValue == "" {
		http.Error(w, "no session", http.StatusUnauthorized)
		return nil
	}

	claims, err := kaipreview.VerifySessionJWT(h.deps.SessionKey, h.deps.Clock, cookieValue)
	if err != nil {
		h.deps.Logger.With(
			attribute.String("appID", h.deps.AppID),
			attribute.String("error", err.Error()),
		).Warn(r.Context(), "kai-preview: session cookie verify failed")
		http.Error(w, "session expired", http.StatusUnauthorized)
		return nil //nolint:nilerr // intentional: invalid session handled via HTTP 401 response
	}
	if claims.AppID != h.deps.AppID || claims.ProjectID != h.deps.AppProjectID {
		h.deps.Logger.With(
			attribute.String("appID", h.deps.AppID),
			attribute.String("cookieAppID", claims.AppID),
		).Warn(r.Context(), "kai-preview: session cookie scope mismatch")
		http.Error(w, "session scope mismatch", http.StatusForbidden)
		return nil
	}

	newJWT, err := kaipreview.MintSessionJWT(h.deps.SessionKey, h.deps.Clock, h.deps.AppID, h.deps.AppProjectID, h.deps.SessionTTL)
	if err != nil {
		return errors.Errorf("kai-preview: re-mint session JWT: %w", err)
	}
	kaipreview.SetSessionCookie(w, newJWT, h.deps.SessionTTL)

	h.deps.Logger.With(
		attribute.String("appID", h.deps.AppID),
		attribute.String("projectID", h.deps.AppProjectID),
	).Debug(r.Context(), "kai-preview: refreshed session cookie")

	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
	return nil
}
