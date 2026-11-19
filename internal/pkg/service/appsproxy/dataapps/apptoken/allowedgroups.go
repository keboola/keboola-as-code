package apptoken

import (
	"slices"
	"strings"

	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

const gitLabProjectPrefix = "project:"

// allowedGroups is the group list oauth2-proxy checks on every request (ProviderData.Authorize): the session must hold
// one of them. For GitLab it is the GitLab groups and projects only. Empty means no requirement.
func allowedGroups(p provider.OAuthProvider) ([]string, error) {
	opts, err := p.ProxyProviderOptions()
	if err != nil {
		return nil, err
	}
	if opts.Type != proxyOptions.GitLabProvider {
		return nonEmpty(opts.AllowedGroups), nil
	}
	groups := nonEmpty(opts.GitLabConfig.Group)
	for _, project := range opts.GitLabConfig.Projects {
		name, _, _ := strings.Cut(project, "=")
		groups = append(groups, gitLabProjectPrefix+name)
	}
	return groups, nil
}

func nonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func matchingGroups(groups, allowed []string) []string {
	var matched []string
	for _, g := range groups {
		if slices.Contains(allowed, g) && !slices.Contains(matched, g) {
			matched = append(matched, g)
		}
	}
	return matched
}

func oauthProvider(app api.AppConfig, id provider.ID) (provider.OAuthProvider, bool) {
	idx := slices.IndexFunc(app.AuthProviders, func(p provider.Provider) bool { return p.ID() == id })
	if idx < 0 {
		return nil, false
	}
	oauth, ok := app.AuthProviders[idx].(provider.OAuthProvider)
	return oauth, ok
}
