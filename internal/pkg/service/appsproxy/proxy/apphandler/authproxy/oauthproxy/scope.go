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
	groupsScope        = "groups"
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

// scope returns the scope to request at login, or "" to keep the oauth2-proxy default of a non-OIDC type.
func (r scopeResolver) scope(ctx context.Context, p options.Provider) string {
	if p.Type != options.OIDCProvider {
		return ""
	}

	supported := r.supportedScopes(ctx, p.OIDCConfig.IssuerURL)

	// Never "" for OIDC: the oauth2-proxy default adds "groups" whenever AllowedGroups is set.
	scopes := []string{"openid", "email", "profile"}
	if len(p.AllowedGroups) > 0 && slices.Contains(supported, groupsScope) {
		scopes = append(scopes, groupsScope)
	}
	if slices.Contains(supported, offlineAccessScope) {
		scopes = append(scopes, offlineAccessScope)
	}
	return strings.Join(scopes, " ")
}

func (r scopeResolver) supportedScopes(ctx context.Context, issuerURL string) []string {
	// The handler built here is cached, so a cancelled triggering request must not decide its scope.
	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), discoveryTimeout, errors.New("OIDC discovery timeout"))
	defer cancel()

	doc := &discoveryDocument{}
	_, _, err := request.NewHTTPRequest(r.client).
		WithGet(strings.TrimRight(issuerURL, "/") + "/.well-known/openid-configuration").
		WithResult(doc).
		Send(ctx)
	if err != nil {
		r.logger.Warnf(ctx, `cannot read OIDC discovery of "%s", "%s" and "%s" scopes not requested: %s`, issuerURL, offlineAccessScope, groupsScope, err)
		return nil
	}
	return doc.ScopesSupported
}
