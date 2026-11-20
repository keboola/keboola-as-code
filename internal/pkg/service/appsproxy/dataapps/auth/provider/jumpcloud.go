package provider

import proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

type JumpCloud struct {
	OIDC
}

func (v JumpCloud) ProxyProviderOptions() (proxyOptions.Provider, error) {
	p, err := v.OIDC.ProxyProviderOptions()
	if err != nil {
		return proxyOptions.Provider{}, err
	}

	// JumpCloud doesn't support "select_account" prompt: "Used unknown value '[select_account]' for prompt parameter".
	p.LoginURLParameters = []proxyOptions.LoginURLParameter{promptParameter(PromptLogin)}

	return p, nil
}
