package provider

import proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

type GitHub struct {
	Base
	URL          string   `json:"url"`
	Organization string   `json:"organization"`
	Team         string   `json:"team"`
	Repository   string   `json:"repository"`
	Token        string   `json:"token"`
	Users        []string `json:"users"`
}

func (v GitHub) ProxyProviderOptions() (proxyOptions.Provider, error) {
	p := proxyOptions.Provider{
		ID:   v.ID().String(),
		Type: proxyOptions.GitHubProvider,
		Name: v.Name(),
		LoginURLParameters: []proxyOptions.LoginURLParameter{
			{
				Name:    "allow_signup",
				Default: []string{"false"},
			},
		},
		GitHubConfig: proxyOptions.GitHubOptions{
			Org:   v.Organization,
			Team:  v.Team,
			Repo:  v.Repository,
			Token: v.Token,
			Users: v.Users,
		},
	}

	// For GitHub Enterprise
	if v.URL != "" {
		p.LoginURL = v.URL + "/login/oauth/authorize"
		p.RedeemURL = v.URL + "/login/oauth/access_token"
		p.ValidateURL = v.URL + "/api/v3"
	}

	return p, nil
}
