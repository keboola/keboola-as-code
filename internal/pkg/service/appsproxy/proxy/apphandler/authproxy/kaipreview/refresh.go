package kaipreview

import (
	"net/http"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type RefreshDeps struct {
	Clock        clockwork.Clock
	DevMode      DevModeChecker
	SessionKey   string
	SessionTTL   time.Duration
	CORS         *CORS
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
		http.NotFound(w, r)
		return nil
	}

	cookieValue := ReadSessionCookie(r)
	if cookieValue == "" {
		http.Error(w, "no session", http.StatusUnauthorized)
		return nil
	}

	claims, err := VerifySessionJWT(h.deps.SessionKey, h.deps.Clock, cookieValue)
	if err != nil {
		http.Error(w, "session expired", http.StatusUnauthorized)
		return nil
	}
	if claims.AppID != h.deps.AppID || claims.ProjectID != h.deps.AppProjectID {
		http.Error(w, "session scope mismatch", http.StatusForbidden)
		return nil
	}

	newJWT, err := MintSessionJWT(h.deps.SessionKey, h.deps.Clock, h.deps.AppID, h.deps.AppProjectID, h.deps.SessionTTL)
	if err != nil {
		return errors.Errorf("kai-preview: re-mint session JWT: %w", err)
	}
	SetSessionCookie(w, newJWT, h.deps.SessionTTL)

	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
	return nil
}
