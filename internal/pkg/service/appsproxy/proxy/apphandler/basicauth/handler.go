package basicauth

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

type Handler struct {
	provider provider.Provider
}

func (h *Handler) ID() provider.ID {
	return h.provider.ID()
}

func (h *Handler) CookieExpiration() time.Duration {
	return 5 * time.Minute
}
