package dependencies

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// projectScope dependencies container implements ProjectScope interface.
type projectScope struct {
	token             keboola.Token
	projectFeatures   keboola.FeaturesMap
	keboolaProjectAPI *keboola.AuthorizedAPI
}

type projectScopeDeps interface {
	BaseScope
	PublicScope
}

type projectScopeConfig struct {
	withoutMasterToken bool
}

type ProjectScopeOption func(c *projectScopeConfig)

// WithoutMasterToken disables the requirement to provide a master token any valid token will be accepted.
func WithoutMasterToken() ProjectScopeOption {
	return func(c *projectScopeConfig) {
		c.withoutMasterToken = true
	}
}

func newProjectConfig(opts []ProjectScopeOption) projectScopeConfig {
	cfg := projectScopeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func NewProjectDeps(ctx context.Context, prjScp projectScopeDeps, tokenStr string, opts ...ProjectScopeOption) (v ProjectScope, err error) {
	ctx, span := prjScp.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewProjectScope")
	defer span.End(&err)

	// Set attributes before token verification
	reqSpan, _ := middleware.RequestSpan(ctx)
	if reqSpan != nil {
		_, stack, _ := strings.Cut(prjScp.StorageAPIHost(), ".")
		tokenID, _, _ := strings.Cut(tokenStr, "-")
		tokenID = strhelper.Truncate(tokenID, 10, "…")
		reqSpan.SetAttributes(
			attribute.String("keboola.project.stack", stack),
			attribute.String("keboola.storage.token.id", tokenID),
		)
	}

	// Verify token
	token, err := prjScp.KeboolaPublicAPI().VerifyTokenRequest(tokenStr).Send(ctx)
	if err != nil {
		return nil, err
	}

	ctx = ctxattr.ContextWith(
		ctx,
		attribute.String("projectId", cast.ToString(token.Owner.ID)),
		attribute.String("tokenId", token.ID),
	)

	prjScp.Logger().Debugf(ctx, "Storage API token is valid.")
	prjScp.Logger().Debugf(ctx, `Project id: "%d", project name: "%s".`, token.ProjectID(), token.ProjectName())

	// Set attributes after token verification
	if reqSpan != nil {
		reqSpan.SetAttributes(
			attribute.String("keboola.project.id", cast.ToString(token.Owner.ID)),
			attribute.String("keboola.project.name", token.Owner.Name),
			attribute.String("keboola.storage.token.id", token.ID),
			attribute.String("keboola.storage.token.description", token.Description),
			attribute.Bool("keboola.storage.token.is_master", token.IsMaster),
		)
	}

	return newProjectScope(ctx, prjScp, *token, opts...)
}

func newProjectScope(ctx context.Context, prjScp projectScopeDeps, token keboola.Token, opts ...ProjectScopeOption) (*projectScope, error) {
	cfg := newProjectConfig(opts)

	// Require master token
	if !cfg.withoutMasterToken && !token.IsMaster {
		return nil, MasterTokenRequiredError{}
	}

	httpClient := prjScp.HTTPClient()
	api, err := keboola.NewAuthorizedAPI(ctx, prjScp.StorageAPIHost(), token.Token, keboola.WithClient(&httpClient), keboola.WithOnSuccessTimeout(1*time.Minute))
	if err != nil {
		return nil, err
	}
	v := &projectScope{
		token:             token,
		projectFeatures:   token.Owner.Features.ToMap(),
		keboolaProjectAPI: api,
	}

	return v, nil
}

func (v *projectScope) check() {
	if v == nil {
		panic(errors.New("dependencies project scope is not initialized"))
	}
}

func (v *projectScope) ProjectBackends() []string {
	var backends []string

	if v.token.Owner.HasSnowflake {
		backends = append(backends, project.BackendSnowflake)
	}
	if v.token.Owner.HasBigquery {
		backends = append(backends, project.BackendBigQuery)
	}
	return backends
}

func (v *projectScope) FileStorageProvider() string {
	return v.token.Owner.FileStorageProvider
}

func (v *projectScope) ProjectID() keboola.ProjectID {
	v.check()
	return keboola.ProjectID(v.token.ProjectID())
}

func (v *projectScope) ProjectName() string {
	v.check()
	return v.token.ProjectName()
}

func (v *projectScope) ProjectFeatures() keboola.FeaturesMap {
	v.check()
	return v.projectFeatures
}

func (v *projectScope) StorageAPIToken() keboola.Token {
	v.check()
	return v.token
}

func (v *projectScope) StorageAPITokenID() string {
	v.check()
	return v.token.ID
}

func (v *projectScope) KeboolaProjectAPI() *keboola.AuthorizedAPI {
	v.check()
	return v.keboolaProjectAPI
}

func (v *projectScope) ObjectIDGeneratorFactory() func(ctx context.Context) *keboola.TicketProvider {
	v.check()
	return func(ctx context.Context) *keboola.TicketProvider {
		return keboola.NewTicketProvider(ctx, v.KeboolaProjectAPI())
	}
}

type MasterTokenRequiredError struct{}

func (MasterTokenRequiredError) StatusCode() int {
	return http.StatusUnauthorized
}

func (MasterTokenRequiredError) Error() string {
	return "a master token of a project administrator is required"
}

func (MasterTokenRequiredError) ErrorName() string {
	return "masterTokenRequired"
}

func (MasterTokenRequiredError) ErrorUserMessage() string {
	return "Please provide a master token of a project administrator."
}
