package dependencies

import (
	"context"
	"net/http"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// project dependencies container implements Project interface.
type project struct {
	base              Base
	public            Public
	token             keboola.Token
	projectFeatures   keboola.FeaturesMap
	keboolaProjectAPI *keboola.API
}

type projectDepsConfig struct {
	withoutMasterToken bool
}

type ProjectDepsOption func(c *projectDepsConfig)

// WithoutMasterToken disables the requirement to provide a master token any valid token will be accepted.
func WithoutMasterToken() ProjectDepsOption {
	return func(c *projectDepsConfig) {
		c.withoutMasterToken = true
	}
}

func NewProjectDeps(ctx context.Context, base Base, public Public, tokenStr string, opts ...ProjectDepsOption) (v Project, err error) {
	ctx, span := base.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewProjectDeps")
	defer telemetry.EndSpan(span, &err)

	// Set attributes before token verification
	reqSpan, _ := middleware.RequestSpan(ctx)
	if reqSpan != nil {
		_, stack, _ := strings.Cut(public.StorageAPIHost(), ".")
		tokenID, _, _ := strings.Cut(tokenStr, "-")
		tokenID = strhelper.Truncate(tokenID, 10, "…")
		reqSpan.SetAttributes(
			attribute.String("keboola.project.stack", stack),
			attribute.String("keboola.storage.token.id", tokenID),
		)
	}

	// Verify token
	token, err := public.KeboolaPublicAPI().VerifyTokenRequest(tokenStr).Send(ctx)
	if err != nil {
		return nil, err
	}

	config := &projectDepsConfig{}
	for _, opt := range opts {
		opt(config)
	}

	base.Logger().Debugf("Storage API token is valid.")
	base.Logger().Debugf(`Project id: "%d", project name: "%s".`, token.ProjectID(), token.ProjectName())

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

	return newProjectDeps(ctx, base, public, *token, config)
}

func newProjectDeps(ctx context.Context, base Base, public Public, token keboola.Token, config *projectDepsConfig) (*project, error) {
	// Require master token
	if !config.withoutMasterToken && !token.IsMaster {
		return nil, MasterTokenRequiredError{}
	}

	httpClient := base.HTTPClient()
	api, err := keboola.NewAPI(ctx, public.StorageAPIHost(), keboola.WithClient(&httpClient), keboola.WithToken(token.Token))
	if err != nil {
		return nil, err
	}
	v := &project{
		base:              base,
		public:            public,
		token:             token,
		projectFeatures:   token.Owner.Features.ToMap(),
		keboolaProjectAPI: api,
	}

	return v, nil
}

func (v project) ProjectID() keboola.ProjectID {
	return keboola.ProjectID(v.token.ProjectID())
}

func (v project) ProjectName() string {
	return v.token.ProjectName()
}

func (v project) ProjectFeatures() keboola.FeaturesMap {
	return v.projectFeatures
}

func (v project) StorageAPIToken() keboola.Token {
	return v.token
}

func (v project) StorageAPITokenID() string {
	return v.token.ID
}

func (v project) KeboolaProjectAPI() *keboola.API {
	return v.keboolaProjectAPI
}

func (v project) ObjectIDGeneratorFactory() func(ctx context.Context) *keboola.TicketProvider {
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
