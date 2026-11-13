package oauthproxy

import (
	"context"
	"net/http"

	"github.com/felixge/httpsnoop"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/sessions/cookie"
	redisstore "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/sessions/redis"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/sessionstore"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// cookieStoreMigration moves a valid cookie-store session into the redis store, so a switch of stores costs no login.
type cookieStoreMigration struct {
	logger       log.Logger
	proxyConfig  *options.Options
	metrics      *sessionstore.Metrics
	cookieStore  sessionsapi.SessionStore
	redisStore   sessionsapi.SessionStore
	signOutPath  string
	callbackPath string
}

func newCookieStoreMigration(logger log.Logger, proxyConfig *options.Options, sessions SessionStore) (*cookieStoreMigration, error) {
	cookieOpts := proxyConfig.Cookie
	cookieOpts.Name = CookieName
	cookieStore, err := cookie.NewCookieSessionStore(&options.SessionOptions{Type: options.CookieSessionStoreType}, &cookieOpts)
	if err != nil {
		return nil, err
	}
	redisStore, err := redisstore.NewRedisSessionStore(&proxyConfig.Session, &proxyConfig.Cookie)
	if err != nil {
		return nil, err
	}
	return &cookieStoreMigration{
		logger:       logger,
		proxyConfig:  proxyConfig,
		metrics:      sessions.Metrics,
		cookieStore:  cookieStore,
		redisStore:   redisStore,
		signOutPath:  proxyConfig.ProxyPrefix + "/sign_out",
		callbackPath: proxyConfig.ProxyPrefix + "/callback",
	}, nil
}

// serve returns the writer for oauth2-proxy, and true when it has written the response.
func (m *cookieStoreMigration) serve(w http.ResponseWriter, req *http.Request) (http.ResponseWriter, bool) {
	m.renameCSRF(req)
	if m.migrate(w, req) {
		return w, true
	}
	return m.wrap(w, req), false
}

// wrap adjusts the cookies of oauth2-proxy's response before its headers are written.
func (m *cookieStoreMigration) wrap(w http.ResponseWriter, req *http.Request) http.ResponseWriter {
	done := false
	beforeHeaders := func() {
		if done {
			return
		}
		done = true
		m.copyCSRF(w)
		m.clearWithTicket(w, req)
	}
	return httpsnoop.Wrap(w, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(code int) {
				beforeHeaders()
				next(code)
			}
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(b []byte) (int, error) {
				beforeHeaders()
				return next(b)
			}
		},
	})
}

// clearWithTicket expires the cookie-store session when the response expires the ticket, so a cleared session is not moved again.
func (m *cookieStoreMigration) clearWithTicket(w http.ResponseWriter, req *http.Request) {
	ticket, ok := responseCookie(w, redisSessionCookieName)
	if !ok || ticket.MaxAge >= 0 {
		return
	}
	m.clearCookieStore(req.Context(), w, req)
}

func (m *cookieStoreMigration) migrate(w http.ResponseWriter, req *http.Request) bool {
	if !hasCookie(req, CookieName) && !hasCookie(req, CookieName+"_0") {
		return false
	}
	if req.URL.Path == m.signOutPath {
		m.clearCookieStore(req.Context(), w, req)
		return false
	}
	if hasCookie(req, redisSessionCookieName) {
		return false
	}

	session, err := m.cookieStore.Load(req)
	if err != nil || session == nil {
		return false
	}

	if err := m.redisStore.Save(w, req, session); err != nil {
		if errors.Is(err, sessionsapi.ErrStoreUnavailable) {
			m.proxyConfig.OnSessionStoreError(w, req, err)
			return true
		}
		m.logger.Warnf(req.Context(), "cannot move a cookie-store session into the session store: %s", err)
		return false
	}

	ticket, ok := responseCookie(w, redisSessionCookieName)
	if !ok {
		m.logger.Warnf(req.Context(), "cannot move a cookie-store session into the session store: no ticket cookie")
		return false
	}
	req.AddCookie(&http.Cookie{Name: ticket.Name, Value: ticket.Value}) //nolint:gosec // G124: a request cookie carries only the name and value
	m.metrics.Migrated(req.Context())
	return false
}

func (m *cookieStoreMigration) clearCookieStore(ctx context.Context, w http.ResponseWriter, req *http.Request) {
	if err := m.cookieStore.Clear(w, req); err != nil {
		m.logger.Warnf(ctx, "cannot clear the cookie-store session: %s", err)
	}
}

func hasCookie(req *http.Request, name string) bool {
	_, err := req.Cookie(name)
	return err == nil
}

func responseCookie(w http.ResponseWriter, name string) (*http.Cookie, bool) {
	var found *http.Cookie
	for _, line := range w.Header().Values("Set-Cookie") {
		c, err := http.ParseSetCookie(line)
		if err == nil && c.Name == name {
			found = c
		}
	}
	return found, found != nil
}
