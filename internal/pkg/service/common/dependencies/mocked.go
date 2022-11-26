package dependencies

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

// mocked dependencies container implements Mocked interface.
type mocked struct {
	*base
	*public
	*project
	envs                *env.Map
	options             *options.Options
	serverWg            *sync.WaitGroup
	debugLogger         log.DebugLogger
	mockedHTTPTransport *httpmock.MockTransport
}

type MockedValues struct {
	Services                           storageapi.Services
	Features                           storageapi.Features
	Components                         storageapi.Components
	StorageAPIHost                     string
	StorageAPIToken                    storageapi.Token
	StorageAPITokenMockedResponseTimes int
}

type MockedOption func(values *MockedValues)

func WithMockedServices(services storageapi.Services) MockedOption {
	return func(values *MockedValues) {
		values.Services = services
	}
}

func WithMockedFeatures(features storageapi.Features) MockedOption {
	return func(values *MockedValues) {
		values.Features = features
	}
}

func WithMockedComponents(components storageapi.Components) MockedOption {
	return func(values *MockedValues) {
		values.Components = components
	}
}

func WithMockedStorageAPIHost(host string) MockedOption {
	return func(values *MockedValues) {
		values.StorageAPIHost = host
	}
}

func WithMockedStorageAPIToken(token storageapi.Token) MockedOption {
	return func(values *MockedValues) {
		values.StorageAPIToken = token
	}
}

func WithMockedTokenResponse(times int) MockedOption {
	return func(values *MockedValues) {
		values.StorageAPITokenMockedResponseTimes = times
	}
}

func NewMockedDeps(opts ...MockedOption) Mocked {
	ctx := context.Background()
	envs := env.Empty()
	logger := log.NewDebugLogger()
	httpClient, mockedHTTPTransport := client.NewMockedClient()

	// Default values
	values := MockedValues{
		Services: storageapi.Services{
			{ID: "encryption", URL: "https://encryption.mocked.transport.http"},
			{ID: "scheduler", URL: "https://scheduler.mocked.transport.http"},
			{ID: "queue", URL: "https://queue.mocked.transport.http"},
			{ID: "sandboxes", URL: "https://sandboxes.mocked.transport.http"},
		},
		Features:       storageapi.Features{"FeatureA", "FeatureB"},
		Components:     testapi.MockedComponents(),
		StorageAPIHost: "mocked.transport.http",
		StorageAPIToken: storageapi.Token{
			ID:       "token-12345-id",
			Token:    "my-secret",
			IsMaster: true,
			Owner: storageapi.TokenOwner{
				ID:       12345,
				Name:     "Project 12345",
				Features: storageapi.Features{"my-feature"},
			},
		},
		StorageAPITokenMockedResponseTimes: 1,
	}

	// Apply options
	for _, opt := range opts {
		opt(&values)
	}

	// Mock API index
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/", values.StorageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, &storageapi.IndexComponents{
			Index: storageapi.Index{Services: values.Services, Features: values.Features}, Components: values.Components,
		}).Once(),
	)

	// Mocked token verification
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/tokens/verify", values.StorageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, values.StorageAPIToken).Times(values.StorageAPITokenMockedResponseTimes),
	)

	// Create base, public and project dependencies
	baseDeps := newBaseDeps(envs, nil, logger, httpClient)
	publicDeps, err := newPublicDeps(ctx, baseDeps, values.StorageAPIHost)
	if err != nil {
		panic(err)
	}
	projectDeps, err := newProjectDeps(baseDeps, publicDeps, values.StorageAPIToken)
	if err != nil {
		panic(err)
	}

	// Clear logs
	logger.Truncate()

	return &mocked{
		base:                baseDeps,
		public:              publicDeps,
		project:             projectDeps,
		envs:                envs,
		options:             options.New(),
		serverWg:            &sync.WaitGroup{},
		debugLogger:         logger,
		mockedHTTPTransport: mockedHTTPTransport,
	}
}

// SetFromTestProject set test dependencies from a testing project.
func (v *mocked) SetFromTestProject(project *testproject.Project) {
	v.storageAPIHost = project.StorageAPIHost()
	v.public.storageAPIClient = project.StorageAPIClient()
	v.project.storageAPIClient = project.StorageAPIClient()
	v.project.token = *project.StorageAPIToken()
	v.encryptionAPIClient = project.EncryptionAPIClient()
	v.schedulerAPIClient = project.SchedulerAPIClient()
	v.mockedHTTPTransport = nil
}

func (v *mocked) EnvsMutable() *env.Map {
	return v.envs
}

func (v *mocked) Options() *options.Options {
	return v.options
}

func (v *mocked) DebugLogger() log.DebugLogger {
	return v.debugLogger
}

func (v *mocked) ServerWaitGroup() *sync.WaitGroup {
	return v.serverWg
}

func (v *mocked) MockedHTTPTransport() *httpmock.MockTransport {
	return v.mockedHTTPTransport
}

func (v *mocked) MockedProject(fs filesystem.Fs) *projectPkg.Project {
	prj, err := projectPkg.New(context.Background(), fs, false)
	if err != nil {
		panic(err)
	}
	return prj
}

func (v *mocked) MockedState() *state.State {
	s, err := state.New(context.Background(), NewObjectsContainer(aferofs.NewMemoryFs(filesystem.WithLogger(v.debugLogger)), fixtures.NewManifest()), v)
	if err != nil {
		panic(err)
	}
	return s
}

// ObjectsContainer implementation for tests.
type ObjectsContainer struct {
	FsValue       filesystem.Fs
	ManifestValue manifest.Manifest
}

func NewObjectsContainer(fs filesystem.Fs, m manifest.Manifest) *ObjectsContainer {
	return &ObjectsContainer{
		FsValue:       fs,
		ManifestValue: m,
	}
}

func (c *ObjectsContainer) Ctx() context.Context {
	return context.Background()
}

func (c *ObjectsContainer) ObjectsRoot() filesystem.Fs {
	return c.FsValue
}

func (c *ObjectsContainer) Manifest() manifest.Manifest {
	return c.ManifestValue
}

func (c *ObjectsContainer) MappersFor(_ *state.State) (mapper.Mappers, error) {
	return mapper.Mappers{}, nil
}
