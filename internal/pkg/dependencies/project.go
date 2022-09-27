package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// project dependencies container implements Project interface.
type project struct {
	base               Base
	public             Public
	token              storageapi.Token
	projectFeatures    storageapi.FeaturesMap
	storageApiClient   client.Client
	schedulerApiClient client.Client
	jobsQueueAPIClient client.Client
	sandboxesApiClient client.Client
	eventSender        event.Sender
}

func NewProjectDeps(ctx context.Context, base Base, public Public, tokenStr string) (v Project, err error) {
	ctx, span := base.Tracer().Start(ctx, "kac.lib.dependencies.NewProjectDeps")
	defer telemetry.EndSpan(span, &err)

	token, err := storageapi.VerifyTokenRequest(tokenStr).Send(ctx, public.StorageApiPublicClient())
	if err != nil {
		return nil, err
	}

	base.Logger().Debugf("Storage API token is valid.")
	base.Logger().Debugf(`Project id: "%d", project name: "%s".`, token.ProjectID(), token.ProjectName())
	return newProjectDeps(base, public, *token)
}

func newProjectDeps(base Base, public Public, token storageapi.Token) (*project, error) {
	v := &project{
		base:             base,
		public:           public,
		token:            token,
		projectFeatures:  token.Owner.Features.ToMap(),
		storageApiClient: storageapi.ClientWithHostAndToken(base.HttpClient(), public.StorageApiHost(), token.Token),
	}

	// Setup Scheduler API
	if schedulerHost, found := v.public.StackServices().URLByID("scheduler"); !found {
		return nil, fmt.Errorf("scheduler host not found")
	} else {
		v.schedulerApiClient = schedulerapi.ClientWithHostAndToken(v.base.HttpClient(), schedulerHost.String(), v.token.Token)
	}

	if queueHost, found := v.public.StackServices().URLByID("queue"); !found {
		return nil, fmt.Errorf("queue host not found")
	} else {
		v.jobsQueueAPIClient = jobsqueueapi.ClientWithHostAndToken(v.base.HttpClient(), queueHost.String(), v.token.Token)
	}

	if sandboxesHost, found := v.public.StackServices().URLByID("sandboxes"); !found {
		return nil, fmt.Errorf("sandboxes host not found")
	} else {
		v.sandboxesApiClient = sandboxesapi.ClientWithHostAndToken(v.base.HttpClient(), sandboxesHost.String(), v.token.Token)
	}

	// Setup event sender
	v.eventSender = event.NewSender(v.base.Logger(), v.StorageApiClient(), v.ProjectID())

	return v, nil
}

func (v project) ProjectID() int {
	return v.token.ProjectID()
}

func (v project) ProjectName() string {
	return v.token.ProjectName()
}

func (v project) ProjectFeatures() storageapi.FeaturesMap {
	return v.projectFeatures
}

func (v project) StorageApiTokenID() string {
	return v.token.ID
}

func (v project) StorageApiClient() client.Sender {
	return v.storageApiClient
}

func (v project) SchedulerApiClient() client.Sender {
	return v.schedulerApiClient
}

func (v project) JobsQueueApiClient() client.Sender {
	return v.jobsQueueAPIClient
}

func (v project) SandboxesApiClient() client.Sender {
	return v.sandboxesApiClient
}

func (v project) EventSender() event.Sender {
	return v.eventSender
}

func (v project) ObjectIDGeneratorFactory() func(ctx context.Context) *storageapi.TicketProvider {
	return func(ctx context.Context) *storageapi.TicketProvider {
		return storageapi.NewTicketProvider(ctx, v.StorageApiClient())
	}
}
