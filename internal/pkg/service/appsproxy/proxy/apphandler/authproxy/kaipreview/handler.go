package kaipreview

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

// PathPrefix is the URL prefix all kai-preview endpoints live under.
var PathPrefix = config.InternalPrefix + "/kai-preview"

const (
	pathEmbedToken = "/embed-token"
	pathBootstrap  = "/bootstrap"
	pathExchange   = "/exchange"
	pathRefresh    = "/refresh"
)

// DevModeCheckerFunc adapts a plain function to the DevModeChecker interface.
type DevModeCheckerFunc func(ctx context.Context, appID string) bool

// IsDevMode implements DevModeChecker.
func (f DevModeCheckerFunc) IsDevMode(ctx context.Context, appID string) bool { return f(ctx, appID) }

type HandlerDeps struct {
	Clock             clockwork.Clock
	STA               STATokenVerifier
	DevMode           DevModeChecker
	CORS              *CORS
	HandshakeKey      string
	SessionKey        string
	SessionTTL        time.Duration
	AllowedOrigins []string
	AppID             string
	AppProjectID      string
}

// Handler is the per-app composite handler that serves all four kai-preview
// internal endpoints. One instance per app. Routes by URL path suffix.
type Handler struct {
	embedToken *EmbedTokenHandler
	bootstrap  *BootstrapHandler
	exchange   *ExchangeHandler
	refresh    *RefreshHandler
}

func NewHandler(deps HandlerDeps) *Handler {
	return &Handler{
		embedToken: NewEmbedTokenHandler(EmbedTokenDeps{
			Clock: deps.Clock, STA: deps.STA, DevMode: deps.DevMode, CORS: deps.CORS,
			HandshakeKey: deps.HandshakeKey, AppID: deps.AppID, AppProjectID: deps.AppProjectID,
		}),
		bootstrap: NewBootstrapHandler(deps.AllowedOrigins, deps.DevMode, deps.AppID),
		exchange: NewExchangeHandler(ExchangeDeps{
			Clock: deps.Clock, DevMode: deps.DevMode,
			HandshakeKey: deps.HandshakeKey, SessionKey: deps.SessionKey, SessionTTL: deps.SessionTTL,
			AppID: deps.AppID, AppProjectID: deps.AppProjectID,
		}),
		refresh: NewRefreshHandler(RefreshDeps{
			Clock: deps.Clock, DevMode: deps.DevMode,
			SessionKey: deps.SessionKey, SessionTTL: deps.SessionTTL, CORS: deps.CORS,
			AppID: deps.AppID, AppProjectID: deps.AppProjectID,
		}),
	}
}

func (h *Handler) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error {
	if !strings.HasPrefix(r.URL.Path, PathPrefix) {
		http.NotFound(w, r)
		return nil
	}
	sub := r.URL.Path[len(PathPrefix):]
	switch sub {
	case pathEmbedToken:
		return h.embedToken.ServeHTTPOrError(w, r)
	case pathBootstrap:
		return h.bootstrap.ServeHTTPOrError(w, r)
	case pathExchange:
		return h.exchange.ServeHTTPOrError(w, r)
	case pathRefresh:
		return h.refresh.ServeHTTPOrError(w, r)
	default:
		http.NotFound(w, r)
		return nil
	}
}
