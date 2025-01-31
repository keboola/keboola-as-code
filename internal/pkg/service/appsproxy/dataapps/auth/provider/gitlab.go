package provider

import proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

type GitLab struct {
	OIDC
	Groups   []string `json:"groups"`
	Projects []string `json:"projects"`
}

func (v GitLab) ProxyProviderOptions() (proxyOptions.Provider, error) {
	p, err := v.OIDC.ProxyProviderOptions()
	if err != nil {
		return proxyOptions.Provider{}, err
	}

	p.Type = proxyOptions.GitLabProvider
	p.LoginURLParameters = []proxyOptions.LoginURLParameter{
		{
			// GitLab doesn't support "select_account" prompt
			// https://gitlab.com/gitlab-org/gitlab/-/issues/377368
			Name:    "prompt",
			Default: []string{"login"},
		},
	}
	p.GitLabConfig = proxyOptions.GitLabOptions{
		Group:    v.Groups,
		Projects: v.Projects,
	}

	return p, nil
}
