package oauthproxy

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	offlineAccessScope = "offline_access"
	discoveryTimeout   = 10 * time.Second
)

type scopeResolver struct {
	logger log.Logger
	client client.Client
}

type discoveryDocument struct {
	ScopesSupported []string `json:"scopes_supported"` //nolint:tagliatelle // OIDC discovery field name
}

func newScopeResolver(logger log.Logger, client client.Client) scopeResolver {
	return scopeResolver{logger: logger, client: client}
}

// scope returns the scope to request at login, or "" to keep the oauth2-proxy default.
func (r scopeResolver) scope(ctx context.Context, p options.Provider) string {
	if p.Type != options.OIDCProvider {
		return ""
	}

	// The handler built here is cached, so a cancelled triggering request must not decide its scope.
	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), discoveryTimeout, errors.New("OIDC discovery timeout"))
	defer cancel()

	doc := &discoveryDocument{}
	_, _, err := request.NewHTTPRequest(r.client).
		WithGet(strings.TrimRight(p.OIDCConfig.IssuerURL, "/") + "/.well-known/openid-configuration").
		WithResult(doc).
		Send(ctx)
	if err != nil {
		r.logger.Warnf(ctx, `cannot read OIDC discovery of "%s", "%s" scope not requested: %s`, p.OIDCConfig.IssuerURL, offlineAccessScope, err)
		return ""
	}

	if !slices.Contains(doc.ScopesSupported, offlineAccessScope) {
		return ""
	}

	// Must match the oauth2-proxy OIDC default, which applies only while Scope is empty.
	scope := "openid email profile"
	if len(p.AllowedGroups) > 0 {
		scope += " groups"
	}
	return scope + " " + offlineAccessScope
}
