package main

import (
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"
)

type App struct {
	ID           AppID             `json:"id" validator:"required"`
	Name         string            `json:"name" validator:"required"`
	UpstreamHost string            `json:"upstreamUrl" validator:"required"`
	Provider     *options.Provider `json:"provider,omitempty"`
}

type AppID string

func (v AppID) String() string {
	return string(v)
}

func exampleApps() []App {
	return []App{
		{
			ID:           "public",
			Name:         "Public App",
			UpstreamHost: "localhost:1000",
			Provider:     nil,
		},
		{
			ID:           "oidc",
			Name:         "OIDC Protected App",
			UpstreamHost: "localhost:2000",
			Provider: &options.Provider{
				ID:                  "oidc",
				ClientID:            "oauth2-proxy",
				ClientSecret:        "proxy",
				Type:                options.OIDCProvider,
				CodeChallengeMethod: providers.CodeChallengeMethodS256,
				OIDCConfig: options.OIDCOptions{
					IssuerURL:      "http://localhost:1234",
					EmailClaim:     options.OIDCEmailClaim,
					GroupsClaim:    options.OIDCGroupsClaim,
					AudienceClaims: options.OIDCAudienceClaims,
					UserIDClaim:    options.OIDCEmailClaim,
				},
			},
		},
	}

}
