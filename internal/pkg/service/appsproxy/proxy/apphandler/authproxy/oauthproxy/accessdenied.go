package oauthproxy

import (
	"net/http"
	"net/url"

	middlewareapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/middleware"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

const (
	callbackPath = config.InternalPrefix + "/callback"
	signInPath   = config.InternalPrefix + "/sign_in"
)

type accessDenied struct {
	logger       log.Logger
	pageWriter   *pagewriter.Writer
	app          api.AppConfig
	provider     provider.OAuthProvider
	providerType options.ProviderType
}

func (d accessDenied) handle(w http.ResponseWriter, req *http.Request, session *sessions.SessionState) {
	email := ""
	var groups []string
	if session != nil {
		email = session.Email
		groups = session.Groups
	}
	reason := deniedReason(d.providerType, groups)

	stage := "session"
	if req.URL.Path == callbackPath {
		stage = "callback"
	}

	exceptionID := pagewriter.ExceptionIDPrefix
	if scope := middlewareapi.GetRequestScope(req); scope != nil {
		exceptionID += scope.RequestID
	}

	ctx := ctxattr.ContextWith(
		req.Context(),
		attribute.String("exceptionId", exceptionID),
		attribute.String("proxy.access.provider", d.provider.ID().String()),
		attribute.String("proxy.access.email", email),
		attribute.String("proxy.access.reason", string(reason)),
		attribute.String("proxy.access.stage", stage),
	)
	d.logger.Warn(ctx, "access denied")

	d.pageWriter.WriteAccessDeniedPage(w, req.WithContext(ctx), &d.app, pagewriter.AccessDenied{
		Email:            email,
		ProviderName:     d.provider.Name(),
		Reason:           reason,
		SwitchAccountURL: d.switchAccountURL(req, stage),
		ExceptionID:      exceptionID,
	})
}

func deniedReason(providerType options.ProviderType, groups []string) pagewriter.AccessDeniedReason {
	switch {
	case providerType == options.GitLabProvider:
		return pagewriter.AccessDeniedNotAllowed
	case len(groups) == 0:
		return pagewriter.AccessDeniedNoGroupsClaim
	default:
		return pagewriter.AccessDeniedNotInGroups
	}
}

func (d accessDenied) switchAccountURL(req *http.Request, stage string) string {
	path := signInPath
	if len(d.app.AuthProviders) > 1 {
		path = selector.SelectionPagePath
	}

	query := url.Values{}
	query.Set("prompt", provider.PromptLogin)
	if stage == "session" && !config.IsInternalPath(req.URL.Path) {
		query.Set("rd", req.URL.Path)
	}
	return (&url.URL{Path: path, RawQuery: query.Encode()}).String()
}
