package dependencies

import (
	"context"
	"net/http"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// project dependencies container implements Project interface.
type project struct {
	base              Base
	public            Public
	token             keboola.Token
	projectFeatures   keboola.FeaturesMap
	keboolaProjectAPI *keboola.API
}

func NewProjectDeps(ctx context.Context, base Base, public Public, tokenStr string) (v Project, err error) {
	ctx, span := base.Tracer().Start(ctx, "kac.lib.dependencies.NewProjectDeps")
	defer telemetry.EndSpan(span, &err)

	token, err := public.KeboolaPublicAPI().VerifyTokenRequest(tokenStr).Send(ctx)
	if err != nil {
		return nil, err
	}

	base.Logger().Debugf("Storage API token is valid.")
	base.Logger().Debugf(`Project id: "%d", project name: "%s".`, token.ProjectID(), token.ProjectName())
	return newProjectDeps(ctx, base, public, *token)
}

func newProjectDeps(ctx context.Context, base Base, public Public, token keboola.Token) (*project, error) {
	// Require master token
	if !token.IsMaster {
		return nil, MasterTokenRequiredError{}
	}

	httpClient := base.HTTPClient()
	v := &project{
		base:              base,
		public:            public,
		token:             token,
		projectFeatures:   token.Owner.Features.ToMap(),
		keboolaProjectAPI: keboola.NewAPI(ctx, public.StorageAPIHost(), keboola.WithClient(&httpClient), keboola.WithToken(token.Token)),
	}

	return v, nil
}

func (v project) ProjectID() int {
	return v.token.ProjectID()
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
