package dependencies

import (
	"context"
	"net/http"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// project dependencies container implements Project interface.
type project struct {
	base             Base
	public           Public
	token            keboola.Token
	projectFeatures  keboola.FeaturesMap
	storageAPIClient *keboola.API
}

func NewProjectDeps(ctx context.Context, base Base, public Public, tokenStr string) (v Project, err error) {
	ctx, span := base.Tracer().Start(ctx, "kac.lib.dependencies.NewProjectDeps")
	defer telemetry.EndSpan(span, &err)

	token, err := keboola.VerifyTokenRequest(tokenStr).Send(ctx, public.StorageAPIPublicClient())
	if err != nil {
		return nil, err
	}

	base.Logger().Debugf("Storage API token is valid.")
	base.Logger().Debugf(`Project id: "%d", project name: "%s".`, token.ProjectID(), token.ProjectName())
	return newProjectDeps(base, public, *token)
}

func newProjectDeps(base Base, public Public, token keboola.Token) (*project, error) {
	// Require master token
	if !token.IsMaster {
		return nil, MasterTokenRequiredError{}
	}

	v := &project{
		base:             base,
		public:           public,
		token:            token,
		projectFeatures:  token.Owner.Features.ToMap(),
		storageAPIClient: keboola.ClientWithHostAndToken(base.HTTPClient(), public.StorageAPIHost(), token.Token),
	}

	// Setup Scheduler API
	if schedulerHost, found := v.public.StackServices().URLByID("scheduler"); !found {
		return nil, errors.New("scheduler host not found")
	} else {
		v.schedulerAPIClient = keboola.ClientWithHostAndToken(v.base.HTTPClient(), schedulerHost.String(), v.token.Token)
	}

	if queueHost, found := v.public.StackServices().URLByID("queue"); !found {
		return nil, errors.New("queue host not found")
	} else {
		v.jobsQueueAPIClient = keboola.ClientWithHostAndToken(v.base.HTTPClient(), queueHost.String(), v.token.Token)
	}

	if sandboxesHost, found := v.public.StackServices().URLByID("sandboxes"); !found {
		return nil, errors.New("sandboxes host not found")
	} else {
		v.sandboxesAPIClient = keboola.ClientWithHostAndToken(v.base.HTTPClient(), sandboxesHost.String(), v.token.Token)
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

func (v project) StorageAPIClient() *keboola.API {
	return v.storageAPIClient
}

func (v project) SchedulerAPIClient() client.Sender {
	return v.schedulerAPIClient
}

func (v project) JobsQueueAPIClient() client.Sender {
	return v.jobsQueueAPIClient
}

func (v project) SandboxesAPIClient() client.Sender {
	return v.sandboxesAPIClient
}

func (v project) ObjectIDGeneratorFactory() func(ctx context.Context) *keboola.TicketProvider {
	return func(ctx context.Context) *keboola.TicketProvider {
		return keboola.NewTicketProvider(ctx, v.StorageAPIClient())
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
