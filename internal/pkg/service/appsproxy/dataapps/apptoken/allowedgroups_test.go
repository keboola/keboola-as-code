package apptoken_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

func oidcApp(roles ...string) api.AppConfig {
	app := testApp()
	p := provider.OIDC{Base: provider.Base{Info: provider.Info{ID: testProviderID, Type: provider.TypeOIDC}}, IssuerURL: testIDP + "/"}
	if len(roles) > 0 {
		p.AllowedRoles = &roles
	}
	app.AuthProviders = provider.Providers{p}
	return app
}

func gitLabApp(groups, projects []string) api.AppConfig {
	app := testApp()
	app.AuthProviders = provider.Providers{provider.GitLab{
		OIDC:     provider.OIDC{Base: provider.Base{Info: provider.Info{ID: testProviderID, Type: provider.TypeGitLab}}, IssuerURL: testIDP + "/"},
		Groups:   groups,
		Projects: projects,
	}}
	return app
}

// handshakeAs opens a websocket to app with the given X-Kbc-User-Roles and returns the refresh token.
func (e *testEnv) handshakeAs(app api.AppConfig, roles string) string {
	e.t.Helper()
	e.app = app
	headers := identityHeaders()
	headers["X-Kbc-User-Roles"] = roles
	got := e.serve(testIDP, wsHeaders(headers))
	refresh := got.header.Get(apptoken.RefreshHeaderName)
	require.NotEmpty(e.t, refresh)
	return refresh
}

func TestInjectRefresh_CarriesTheMatchedGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	tok := e.parseRefresh(e.handshakeAs(oidcApp("b", "c"), "a,b"))
	kbc, _ := tok.payload["kbc"].(map[string]any)
	assert.Equal(t, []any{"b"}, kbc["matched_groups"])
}

func TestRefresh_AllowedGroups(t *testing.T) {
	t.Parallel()

	manyGroups := strings.Repeat("filler-group,", 400) + "b"
	cases := []struct {
		name      string
		handshake api.AppConfig
		roles     string
		refresh   api.AppConfig
		allowed   bool
	}{
		{name: "no role requirement", handshake: oidcApp(), roles: "a,b", refresh: oidcApp(), allowed: true},
		{name: "still allowed", handshake: oidcApp("b", "c"), roles: "a,b", refresh: oidcApp("b", "c"), allowed: true},
		{name: "allowed through a role that stays", handshake: oidcApp("a", "b"), roles: "a,b", refresh: oidcApp("b"), allowed: true},
		{name: "role removed", handshake: oidcApp("b", "c"), roles: "a,b", refresh: oidcApp("c")},
		{name: "roles required after the handshake", handshake: oidcApp(), roles: "a,b", refresh: oidcApp("a")},
		{name: "group list too large for the token", handshake: oidcApp("b"), roles: manyGroups, refresh: oidcApp("b"), allowed: true},
		{name: "gitlab group", handshake: gitLabApp([]string{"g1"}, nil), roles: "g1", refresh: gitLabApp([]string{"g1"}, nil), allowed: true},
		{name: "gitlab project with access level", handshake: gitLabApp(nil, []string{"grp/proj=30"}), roles: "project:grp/proj", refresh: gitLabApp(nil, []string{"grp/proj=30"}), allowed: true},
		{name: "gitlab project removed", handshake: gitLabApp([]string{"g1"}, []string{"grp/proj"}), roles: "project:grp/proj", refresh: gitLabApp([]string{"g1"}, nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, envOptions{})
			refresh := e.handshakeAs(tc.handshake, tc.roles)

			token, _, err := e.refresh(tc.refresh, e.ref, refresh)
			if tc.allowed {
				require.NoError(t, err)
				assert.NotEmpty(t, token)
				return
			}
			require.Error(t, err)
			assert.Empty(t, token)
		})
	}
}
