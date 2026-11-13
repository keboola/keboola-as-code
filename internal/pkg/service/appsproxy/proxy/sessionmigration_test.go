package proxy_test

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/oauth2-proxy/mockoidc"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	sessionsapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/encryption"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/sessions/cookie"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const migratedCounter = "keboola.go.appsproxy.sessionstore.migrated"

type migrationEnvs struct {
	cookie *previewEnv
	redis  *previewEnv
}

func startMigrationEnvs(t *testing.T) migrationEnvs {
	t.Helper()
	return migrationEnvs{cookie: startPreviewEnv(t, fixedCookieSalt), redis: startRedisEnv(t, fixedCookieSalt)}
}

// cookieStoreClient logs in at the cookie-mode env and returns a redis-mode client holding the cookie-store session.
func (m migrationEnvs) cookieStoreClient(user *mockoidc.MockUser) *http.Client {
	m.cookie.t.Helper()
	return m.redis.clientWithCookies(m.cookie.oidcLogin(m.cookie.providers[0], user, oidcOrigin+"/"))
}

// oldCookieClient logs in, then returns a client holding only a cookie-store copy of that session, changed by mutate
// and signed with the secret of providerID. The env holds no session key afterwards.
func (e *previewEnv) oldCookieClient(providerID string, mutate func(s *sessionsapi.SessionState)) *http.Client {
	e.t.Helper()
	session := e.loadOIDCSession(e.oidcLogin(e.providers[0], freshnessUser(), oidcOrigin+"/"))
	mutate(session)
	e.redis.FlushAll()

	store, err := cookie.NewCookieSessionStore(&options.SessionOptions{Type: options.CookieSessionStoreType}, &options.Cookie{
		Name:     oauthCookieName,
		Secret:   oidcCookieSecret(e, providerID),
		Domains:  []string{oidcHost},
		Path:     "/",
		Expire:   24 * time.Hour,
		Secure:   true,
		HTTPOnly: true,
		SameSite: "strict",
	})
	require.NoError(e.t, err)
	rec := httptest.NewRecorder()
	require.NoError(e.t, store.Save(rec, httptest.NewRequestWithContext(e.t.Context(), http.MethodGet, oidcOrigin+"/", nil), session))
	client := e.clientWithCookies(nil)
	client.Jar.SetCookies(oidcCookieURL, rec.Result().Cookies())
	require.Equal(e.t, []string{oauthCookieName}, oldCookieNames(client))
	return client
}

func oidcCookieSecret(env *previewEnv, providerID string) string {
	h := sha256.New()
	h.Write([]byte("oidc/" + providerID + "/" + env.d.Config().CookieSecretSalt))
	return fmt.Sprintf("%x", h.Sum(nil)[:16])
}

func (e *previewEnv) requireLoginRedirect(resp *http.Response) {
	e.t.Helper()
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	assert.Contains(e.t, resp.Header.Get("Location"), "/oidc/authorize?client_id=")
}

func oldCookieNames(client *http.Client) []string {
	var names []string
	for _, c := range client.Jar.Cookies(oidcCookieURL) {
		if regexp.MustCompile(`^_oauth2_proxy(_\d+)?$`).MatchString(c.Name) {
			names = append(names, c.Name)
		}
	}
	return names
}

func TestCookieStoreMigration_SingleCookie(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	client := envs.cookieStoreClient(freshnessUser())
	require.Equal(t, []string{oauthCookieName}, oldCookieNames(client))

	resp := envs.redis.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.False(t, expiresCookie(resp, oauthCookieName))
	assert.Contains(t, setCookieNames(resp), ticketCookieName)
	assert.Equal(t, "admin@keboola.com", envs.redis.lastAppRequest().Header.Get("X-Kbc-User-Email"))
	assert.Len(t, envs.redis.ticketKeys(), 1)
	assert.Equal(t, []string{oauthCookieName}, oldCookieNames(client))
	assert.Equal(t, int64(1), envs.redis.counterValue(migratedCounter))

	resp = envs.redis.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, envs.redis.ticketKeys(), 1)
	assert.Equal(t, int64(1), envs.redis.counterValue(migratedCounter))
}

func TestCookieStoreMigration_SplitCookie(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	client := envs.cookieStoreClient(manyGroupsUser(200))
	chunks := oldCookieNames(client)
	require.Contains(t, chunks, oauthCookieName+"_0")
	require.Contains(t, chunks, oauthCookieName+"_1")

	resp := envs.redis.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	for _, name := range chunks {
		assert.False(t, expiresCookie(resp, name), name)
	}
	assert.Contains(t, envs.redis.lastAppRequest().Header.Get("X-Kbc-User-Roles"), "group-with-a-long-name-198")
	assert.Len(t, envs.redis.ticketKeys(), 1)
	assert.ElementsMatch(t, chunks, oldCookieNames(client))
	assert.Equal(t, int64(1), envs.redis.counterValue(migratedCounter))
}

func TestCookieStoreMigration_CookieModeReplicaAfterMove(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	client := envs.cookieStoreClient(freshnessUser())
	require.Equal(t, http.StatusOK, envs.redis.send(client, oidcOrigin+"/some/path").StatusCode)

	cookieClient := envs.cookie.clientWithCookies(client)
	assert.Equal(t, http.StatusOK, envs.cookie.send(cookieClient, oidcOrigin+"/some/path").StatusCode)

	assert.Equal(t, http.StatusOK, envs.redis.send(envs.redis.clientWithCookies(cookieClient), oidcOrigin+"/some/path").StatusCode)
	assert.Len(t, envs.redis.ticketKeys(), 1)
	assert.Equal(t, int64(1), envs.redis.counterValue(migratedCounter))
}

func TestCookieStoreMigration_EvictedKeyClearsOldCookie(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	client := envs.cookieStoreClient(freshnessUser())
	require.Equal(t, http.StatusOK, envs.redis.send(client, oidcOrigin+"/some/path").StatusCode)
	keys := envs.redis.ticketKeys()
	require.Len(t, keys, 1)
	envs.redis.redis.Del(keys[0])

	resp := envs.redis.send(client, oidcOrigin+"/some/path")
	envs.redis.requireLoginRedirect(resp)
	assert.True(t, expiresCookie(resp, ticketCookieName))
	assert.True(t, expiresCookie(resp, oauthCookieName))
	assert.Empty(t, oldCookieNames(client))

	envs.redis.requireLoginRedirect(envs.redis.send(client, oidcOrigin+"/some/path"))
	assert.Empty(t, envs.redis.ticketKeys())
	assert.Equal(t, int64(1), envs.redis.counterValue(migratedCounter))
}

func TestCookieStoreMigration_KeepsCreatedAt(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	createdAt := ago(23 * time.Hour)
	client := env.oldCookieClient("oidc", func(s *sessionsapi.SessionState) {
		s.RefreshToken = ""
		s.CreatedAt = createdAt
	})

	resp := env.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.WithinDuration(t, *createdAt, *env.loadOIDCSession(client).CreatedAt, time.Second)

	ticket := jarCookie(client, ticketCookieName)
	require.NotNil(t, ticket)
	secret := oidcCookieSecret(env, "oidc")
	_, _, ok := encryption.Validate(ticket, secret, 24*time.Hour)
	assert.True(t, ok)
	_, _, ok = encryption.Validate(ticket, secret, 22*time.Hour)
	assert.False(t, ok, "the ticket must expire when the old cookie would have")
}

func TestCookieStoreMigration_RefreshesOldSession(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oldCookieClient("oidc", func(s *sessionsapi.SessionState) {
		require.NotEmpty(t, s.RefreshToken)
		s.CreatedAt = ago(2 * time.Hour)
	})

	resp := env.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.WithinDuration(t, time.Now(), *env.loadOIDCSession(client).CreatedAt, time.Minute)
	assert.Len(t, env.ticketKeys(), 1)
	assert.Equal(t, int64(1), env.counterValue(migratedCounter))
}

func TestCookieStoreMigration_StoreDownKeepsOldCookie(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	client := envs.cookieStoreClient(freshnessUser())

	envs.redis.redis.Close()
	resp := envs.redis.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, readBody(t, resp), "Sign-in is temporarily unavailable. Please try again in a moment.")
	assert.False(t, expiresCookie(resp, oauthCookieName))
	assert.Equal(t, []string{oauthCookieName}, oldCookieNames(client))
	assert.Equal(t, int64(1), envs.redis.counterValue("keboola.go.appsproxy.sessionstore.unavailable"))
	assert.Equal(t, int64(0), envs.redis.counterValue(migratedCounter))

	require.NoError(t, envs.redis.redis.Restart())
	resp = envs.redis.send(client, oidcOrigin+"/some/path")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int64(1), envs.redis.counterValue(migratedCounter))
}

func TestCookieStoreMigration_SignOutAfterMigrationSticks(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	client := envs.cookieStoreClient(freshnessUser())
	require.Equal(t, http.StatusOK, envs.redis.send(client, oidcOrigin+"/some/path").StatusCode)

	envs.redis.send(client, oidcOrigin+"/_proxy/sign_out")
	assert.Empty(t, envs.redis.ticketKeys())
	assert.Empty(t, oldCookieNames(client))

	envs.redis.requireLoginRedirect(envs.redis.send(client, oidcOrigin+"/some/path"))
	assert.Empty(t, envs.redis.ticketKeys())
	assert.Equal(t, int64(1), envs.redis.counterValue(migratedCounter))
}

func TestCookieStoreMigration_SignOutClearsOldCookie(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	client := envs.cookieStoreClient(freshnessUser())

	resp := envs.redis.send(client, oidcOrigin+"/_proxy/sign_out")
	assert.True(t, expiresCookie(resp, oauthCookieName))
	assert.Empty(t, envs.redis.ticketKeys())
	assert.Empty(t, oldCookieNames(client))

	envs.redis.requireLoginRedirect(envs.redis.send(client, oidcOrigin+"/some/path"))
	assert.Equal(t, int64(0), envs.redis.counterValue(migratedCounter))
}

func TestCookieStoreMigration_FatalRefreshLogsOutOnce(t *testing.T) {
	t.Parallel()
	env := startRedisEnv(t)
	client := env.oldCookieClient("oidc", func(s *sessionsapi.SessionState) {
		require.NotEmpty(t, s.RefreshToken)
		s.CreatedAt = ago(2 * time.Hour)
	})
	// The IdP no longer knows the session, so the refresh gets invalid_grant.
	clear(env.providers[0].SessionStore.Store)

	env.requireLoginRedirect(env.send(client, oidcOrigin+"/some/path"))
	assert.Empty(t, oldCookieNames(client))
	assert.Nil(t, jarCookie(client, ticketCookieName))
	assert.Empty(t, env.ticketKeys())

	env.requireLoginRedirect(env.send(client, oidcOrigin+"/some/path"))
	assert.Empty(t, env.ticketKeys())
	assert.Equal(t, int64(1), env.counterValue(migratedCounter))
}

func TestCookieStoreMigration_InvalidOldCookie(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		build func(env *previewEnv) *http.Client
	}{
		{
			name: "tampered",
			build: func(env *previewEnv) *http.Client {
				client := env.oldCookieClient("oidc", func(*sessionsapi.SessionState) {})
				c := jarCookie(client, oauthCookieName)
				parts := strings.Split(c.Value, "|")
				require.Len(env.t, parts, 3)
				c.Value = parts[0] + "|" + parts[1] + "|" + strings.Repeat("A", len(parts[2]))
				client.Jar.SetCookies(oidcCookieURL, []*http.Cookie{c})
				return client
			},
		},
		{
			name: "older than 24 h",
			build: func(env *previewEnv) *http.Client {
				return env.oldCookieClient("oidc", func(s *sessionsapi.SessionState) {
					s.CreatedAt = ago(24*time.Hour + time.Minute)
				})
			},
		},
		{
			name: "other provider",
			build: func(env *previewEnv) *http.Client {
				return env.oldCookieClient("other", func(*sessionsapi.SessionState) {})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := startRedisEnv(t)
			client := tc.build(env)

			env.requireLoginRedirect(env.send(client, oidcOrigin+"/some/path"))
			assert.Empty(t, env.ticketKeys())
			assert.Nil(t, jarCookie(client, ticketCookieName))
			assert.Equal(t, int64(0), env.counterValue(migratedCounter))
		})
	}
}

// shareIDP points the oidc app of env "to" at the IdP of env "from", so a login started at one env can finish at the other.
func shareIDP(from, to *previewEnv) {
	app := to.appsAPI.Apps["oidc"]
	app.AuthProviders = from.appsAPI.Apps["oidc"].AuthProviders
	to.appsAPI.Apps["oidc"] = app
}

// startLogin starts a login at env and returns the callback URL the IdP sends the browser to.
func (e *previewEnv) startLogin(client *http.Client) string {
	e.t.Helper()
	e.providers[0].QueueUser(freshnessUser())
	resp := e.send(client, oidcOrigin+"/")
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	resp = e.send(client, resp.Header.Get("Location"))
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	resp = e.send(client, resp.Header.Get("Location"))
	require.Equal(e.t, http.StatusOK, resp.StatusCode)
	callback := extractMetaRefreshTag(e.t, []byte(readBody(e.t, resp)))
	require.Contains(e.t, callback, "/_proxy/callback")
	return callback
}

func (e *previewEnv) requireLoggedIn(resp *http.Response, client *http.Client) {
	e.t.Helper()
	require.Equal(e.t, http.StatusFound, resp.StatusCode)
	assert.Equal(e.t, "/", resp.Header.Get("Location"))
	assert.Contains(e.t, setCookieNames(resp), e.oidcCookieName())
	assert.Equal(e.t, http.StatusOK, e.send(client, oidcOrigin+"/some/path").StatusCode)
}

func TestCookieStoreMigration_LoginFromCookieModeFinishesInRedisMode(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	shareIDP(envs.cookie, envs.redis)
	starter := envs.cookie.clientWithCookies(nil)
	callback := envs.cookie.startLogin(starter)

	client := envs.redis.clientWithCookies(starter)
	envs.redis.requireLoggedIn(envs.redis.send(client, callback), client)
	assert.Len(t, envs.redis.ticketKeys(), 1)
}

func TestCookieStoreMigration_LoginFromRedisModeFinishesInCookieMode(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	shareIDP(envs.redis, envs.cookie)
	starter := envs.redis.clientWithCookies(nil)
	callback := envs.redis.startLogin(starter)

	client := envs.cookie.clientWithCookies(starter)
	envs.cookie.requireLoggedIn(envs.cookie.send(client, callback), client)
}

func TestCookieStoreMigration_InvalidCookieModeCSRF(t *testing.T) {
	t.Parallel()
	const csrfName = oauthCookieName + "_csrf"
	cases := []struct {
		name   string
		change func(env *previewEnv, c *http.Cookie)
	}{
		{
			name: "tampered",
			change: func(env *previewEnv, c *http.Cookie) {
				parts := strings.Split(c.Value, "|")
				require.Len(env.t, parts, 3)
				c.Value = parts[0] + "|" + parts[1] + "|" + strings.Repeat("A", len(parts[2]))
			},
		},
		{
			name: "expired",
			change: func(env *previewEnv, c *http.Cookie) {
				secret := oidcCookieSecret(env, "oidc")
				value, _, ok := encryption.Validate(c, secret, 15*time.Minute)
				require.True(env.t, ok)
				signed, err := encryption.SignedValue(secret, csrfName, value, time.Now().Add(-16*time.Minute))
				require.NoError(env.t, err)
				c.Value = signed
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			envs := startMigrationEnvs(t)
			shareIDP(envs.cookie, envs.redis)
			starter := envs.cookie.clientWithCookies(nil)
			callback := envs.cookie.startLogin(starter)
			c := jarCookie(starter, csrfName)
			require.NotNil(t, c)
			tc.change(envs.cookie, c)
			starter.Jar.SetCookies(oidcCookieURL, []*http.Cookie{c})

			resp := envs.redis.send(envs.redis.clientWithCookies(starter), callback)
			assert.Equal(t, http.StatusForbidden, resp.StatusCode)
			assert.Empty(t, envs.redis.ticketKeys())
		})
	}
}

func TestCookieStoreMigration_LoginFromCookieModeAfterAbandonedRedisModeLogin(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	shareIDP(envs.cookie, envs.redis)
	abandoned := envs.redis.clientWithCookies(nil)
	require.Equal(t, http.StatusFound, envs.redis.send(abandoned, oidcOrigin+"/").StatusCode)
	require.NotNil(t, jarCookie(abandoned, ticketCookieName+"_csrf"))

	starter := envs.cookie.clientWithCookies(abandoned)
	callback := envs.cookie.startLogin(starter)

	client := envs.redis.clientWithCookies(starter)
	envs.redis.requireLoggedIn(envs.redis.send(client, callback), client)
	assert.Len(t, envs.redis.ticketKeys(), 1)
}

func TestCookieStoreMigration_NoCSRFMatchesState(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	shareIDP(envs.cookie, envs.redis)
	abandoned := envs.redis.clientWithCookies(nil)
	require.Equal(t, http.StatusFound, envs.redis.send(abandoned, oidcOrigin+"/").StatusCode)

	starter := envs.cookie.clientWithCookies(abandoned)
	callback := envs.cookie.startLogin(starter)
	require.Equal(t, http.StatusFound, envs.cookie.send(starter, oidcOrigin+"/").StatusCode)

	resp := envs.redis.send(envs.redis.clientWithCookies(starter), callback)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Empty(t, envs.redis.ticketKeys())
}

func TestCookieStoreMigration_LoginFromRedisModeAfterAbandonedCookieModeLogin(t *testing.T) {
	t.Parallel()
	envs := startMigrationEnvs(t)
	shareIDP(envs.redis, envs.cookie)
	starter := envs.redis.clientWithCookies(nil)
	callback := envs.redis.startLogin(starter)

	abandoned := envs.cookie.clientWithCookies(starter)
	require.Equal(t, http.StatusFound, envs.cookie.send(abandoned, oidcOrigin+"/").StatusCode)

	client := envs.redis.clientWithCookies(abandoned)
	envs.redis.requireLoggedIn(envs.redis.send(client, callback), client)
	assert.Len(t, envs.redis.ticketKeys(), 1)
}
