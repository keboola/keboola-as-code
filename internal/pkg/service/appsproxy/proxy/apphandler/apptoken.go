package apphandler

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type appUserTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expiresAt"`
}

// serveAppUserTokenRefresh takes the refresh token from Authorization: inbound X-Kbc-* headers are stripped before routing.
func (h *appHandler) serveAppUserTokenRefresh(w http.ResponseWriter, req *http.Request) error {
	minter := h.manager.appTokenMinter
	if !minter.RefreshEnabled() {
		return internalPathNotFound(req.URL.Path)
	}

	w.Header().Set("Cache-Control", "no-store")
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return nil
	}

	ctx := req.Context()
	refreshToken, ok := bearerToken(req.Header.Get("Authorization"))
	if !ok {
		h.manager.logger.Info(ctx, "app user token refresh refused: no bearer credential")
		writeRefreshRefused(w)
		return nil
	}

	token, expiresAt, err := minter.Refresh(ctx, h.app, h.workload, refreshToken)
	if err != nil {
		h.manager.logger.Infof(ctx, "app user token refresh refused: %s", err)
		writeRefreshRefused(w)
		return nil
	}

	body, err := json.Marshal(appUserTokenResponse{Token: token, ExpiresAt: expiresAt.UTC().Format(time.RFC3339)})
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
	return nil
}

func writeRefreshRefused(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	w.WriteHeader(http.StatusUnauthorized)
}

func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}
