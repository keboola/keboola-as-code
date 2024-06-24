package basicauth

import (
	"net/http"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/util"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	basicAuthCookie = "proxyBasicAuth"
	formPagePath    = config.InternalPrefix + "/form"
)

type Handler struct {
	logger     log.Logger
	pageWriter *pagewriter.Writer
	clock      clock.Clock
	app        api.AppConfig
	basicAuth  provider.Basic
	upstream   chain.Handler
}

func NewHandler(
	logger log.Logger,
	pageWriter *pagewriter.Writer,
	clock clock.Clock,
	app api.AppConfig,
	auth provider.Basic,
	upstream chain.Handler,
) *Handler {
	return &Handler{
		logger:     logger,
		pageWriter: pageWriter,
		clock:      clock,
		app:        app,
		basicAuth:  auth,
		upstream:   upstream,
	}
}

func (h *Handler) Name() string {
	return h.basicAuth.Name()
}

func (h *Handler) SignInPath() string {
	return formPagePath
}

func (h *Handler) CookieExpiration() time.Duration {
	return 5 * time.Minute
}

func (h *Handler) ServeHTTPOrError(w http.ResponseWriter, req *http.Request) error {
	cookie, _ := req.Cookie(basicAuthCookie)
	if _, ok := req.Form["password"]; !ok || cookie == nil {
		h.pageWriter.WriteFormPage(w, req, http.StatusOK)
		return nil
	}

	if req.URL.Path != formPagePath {
		return nil
	}

	if !h.basicAuth.IsAuthorized("") {
		return errors.New("wrong password prompted")
	}

	host, _ := util.SplitHostPort(req.Host)
	if host == "" {
		panic(errors.New("host cannot be empty"))
	}

	v := &http.Cookie{
		Name:     basicAuthCookie,
		Path:     "/",
		Domain:   host,
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}

	expires := h.CookieExpiration()
	if expires > 0 {
		// If there is an expiration, set it
		v.Expires = h.clock.Now().Add(expires)
	} else {
		// Otherwise clear the cookie
		v.MaxAge = -1
	}

	return h.upstream.ServeHTTPOrError(w, req)
}
