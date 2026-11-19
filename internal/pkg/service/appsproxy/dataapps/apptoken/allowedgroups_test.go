package apptoken_test

import (
	"slices"
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

// handshakeAs opens a websocket to app as a user whose login session holds groups, and returns the refresh token.
// The header carries the groups comma-joined, as oauth2-proxy writes it.
func (e *testEnv) handshakeAs(app api.AppConfig, groups []string) string {
	e.t.Helper()
	e.app = app
	e.sessionGroups = groups
	headers := identityHeaders()
	headers["X-Kbc-User-Roles"] = strings.Join(groups, ",")
	got := e.serve(testIDP, wsHeaders(headers))
	refresh := got.header.Get(apptoken.RefreshHeaderName)
	require.NotEmpty(e.t, refresh)
	return refresh
}

func TestInjectRefresh_CarriesTheMatchedGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	tok := e.parseRefresh(e.handshakeAs(oidcApp("b", "c"), []string{"a", "b"}))
	kbc, _ := tok.payload["kbc"].(map[string]any)
	assert.Equal(t, []any{"b"}, kbc["matched_groups"])
}

func TestRefresh_AllowedGroups(t *testing.T) {
	t.Parallel()

	manyGroups := append(slices.Repeat([]string{"filler-group"}, 400), "b")
	cases := []struct {
		name      string
		handshake api.AppConfig
		groups    []string
		refresh   api.AppConfig
		allowed   bool
	}{
		{name: "no role requirement", handshake: oidcApp(), groups: []string{"a", "b"}, refresh: oidcApp(), allowed: true},
		{name: "still allowed", handshake: oidcApp("b", "c"), groups: []string{"a", "b"}, refresh: oidcApp("b", "c"), allowed: true},
		{name: "allowed through a role that stays", handshake: oidcApp("a", "b"), groups: []string{"a", "b"}, refresh: oidcApp("b"), allowed: true},
		{name: "role removed", handshake: oidcApp("b", "c"), groups: []string{"a", "b"}, refresh: oidcApp("c")},
		{name: "roles required after the handshake", handshake: oidcApp(), groups: []string{"a", "b"}, refresh: oidcApp("a")},
		{name: "group list too large for the token", handshake: oidcApp("b"), groups: manyGroups, refresh: oidcApp("b"), allowed: true},
		{name: "group with a comma is one group", handshake: oidcApp("team,blue"), groups: []string{"team,blue"}, refresh: oidcApp("team,blue"), allowed: true},
		{name: "comma does not create a membership", handshake: oidcApp("old", "admin"), groups: []string{"old", "prefix,admin"}, refresh: oidcApp("admin")},
		{name: "gitlab group", handshake: gitLabApp([]string{"g1"}, nil), groups: []string{"g1"}, refresh: gitLabApp([]string{"g1"}, nil), allowed: true},
		{name: "gitlab project with access level", handshake: gitLabApp(nil, []string{"grp/proj=30"}), groups: []string{"project:grp/proj"}, refresh: gitLabApp(nil, []string{"grp/proj=30"}), allowed: true},
		{name: "gitlab project removed", handshake: gitLabApp([]string{"g1"}, []string{"grp/proj"}), groups: []string{"project:grp/proj"}, refresh: gitLabApp([]string{"g1"}, nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, envOptions{})
			refresh := e.handshakeAs(tc.handshake, tc.groups)

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
