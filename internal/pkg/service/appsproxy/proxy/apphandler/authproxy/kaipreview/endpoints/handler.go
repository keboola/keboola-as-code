package endpoints

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
)

// PathPrefix is the URL prefix all kai-preview endpoints live under.
const PathPrefix = config.InternalPrefix + "/kai-preview"

const (
	pathHandshakeToken = "/handshake-token"
	pathBootstrap      = "/bootstrap"
	pathExchange       = "/exchange"
	pathRefresh        = "/refresh"
)

// DevModeCheckerFunc adapts a plain function to the DevModeChecker interface.
type DevModeCheckerFunc func(ctx context.Context, appID string) bool

// IsDevMode implements DevModeChecker.
func (f DevModeCheckerFunc) IsDevMode(ctx context.Context, appID string) bool { return f(ctx, appID) }

type HandlerDeps struct {
	Logger               log.Logger
	Clock                clockwork.Clock
	StorageTokenVerifier kaipreview.StorageTokenVerifier
	DevMode              DevModeChecker
	CORS                 *kaipreview.CORS
	HandshakeKey         string
	SessionKey           string
	SessionTTL           time.Duration
	// AllowedFrameAncestors controls the bootstrap CSP frame-ancestors and shim origin list.
	// Distinct from AllowedOrigins (which is fed to CORS) to allow different operator configs.
	AllowedFrameAncestors []string
	AppID                 string
	AppProjectID          string
}

// Handler is the per-app composite handler that serves all four kai-preview
// internal endpoints. One instance per app. Routes by URL path suffix.
type Handler struct {
	handshakeToken *HandshakeTokenHandler
	bootstrap      *BootstrapHandler
	exchange       *ExchangeHandler
	refresh        *RefreshHandler
}

func NewHandler(deps HandlerDeps) *Handler {
	return &Handler{
		handshakeToken: NewHandshakeTokenHandler(HandshakeTokenDeps{
			Logger: deps.Logger, Clock: deps.Clock, StorageTokenVerifier: deps.StorageTokenVerifier,
			DevMode: deps.DevMode, CORS: deps.CORS,
			HandshakeKey: deps.HandshakeKey, SessionTTL: deps.SessionTTL,
			AppID: deps.AppID, AppProjectID: deps.AppProjectID,
		}),
		bootstrap: NewBootstrapHandler(deps.AllowedFrameAncestors, deps.DevMode, deps.AppID),
		exchange: NewExchangeHandler(ExchangeDeps{
			Logger: deps.Logger, Clock: deps.Clock, DevMode: deps.DevMode,
			HandshakeKey: deps.HandshakeKey, SessionKey: deps.SessionKey, SessionTTL: deps.SessionTTL,
			AppID: deps.AppID, AppProjectID: deps.AppProjectID,
		}),
		refresh: NewRefreshHandler(RefreshDeps{
			Logger: deps.Logger, Clock: deps.Clock, DevMode: deps.DevMode,
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
	case pathHandshakeToken:
		return h.handshakeToken.ServeHTTPOrError(w, r)
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
