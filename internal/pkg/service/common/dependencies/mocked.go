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
	t                   *testing.T
	envs                *env.Map
	options             *options.Options
	serverWg            *sync.WaitGroup
	debugLogger         log.DebugLogger
	mockedHTTPTransport *httpmock.MockTransport
}

type MockedValues struct {
	services                           storageapi.Services
	features                           storageapi.Features
	components                         storageapi.Components
	storageAPIHost                     string
	storageAPIToken                    storageapi.Token
	storageAPITokenMockedResponseTimes int

	useRealAPIs         bool
	storageAPIClient    client.Client
	encryptionAPIClient client.Client
	schedulerAPIClient  client.Client
}

type MockedOption func(values *MockedValues)

func WithTestProject(project *testproject.Project) MockedOption {
	return func(values *MockedValues) {
		values.storageAPIHost = project.StorageAPIHost()
		values.storageAPIToken = *project.StorageAPIToken()

		values.useRealAPIs = true
		values.storageAPIClient = project.StorageAPIClient()
		values.encryptionAPIClient = project.EncryptionAPIClient()
		values.schedulerAPIClient = project.SchedulerAPIClient()
	}
}

func WithMockedServices(services storageapi.Services) MockedOption {
	return func(values *MockedValues) {
		values.services = services
	}
}

func WithMockedFeatures(features storageapi.Features) MockedOption {
	return func(values *MockedValues) {
		values.features = features
	}
}

func WithMockedComponents(components storageapi.Components) MockedOption {
	return func(values *MockedValues) {
		values.components = components
	}
}

func WithMockedStorageAPIHost(host string) MockedOption {
	return func(values *MockedValues) {
		values.storageAPIHost = host
	}
}

func WithMockedStorageAPIToken(token storageapi.Token) MockedOption {
	return func(values *MockedValues) {
		values.storageAPIToken = token
	}
}

func WithMockedTokenResponse(times int) MockedOption {
	return func(values *MockedValues) {
		values.storageAPITokenMockedResponseTimes = times
	}
}

func NewMockedDeps(t *testing.T, opts ...MockedOption) Mocked {
	t.Helper()
	ctx := context.Background()
	envs := env.Empty()
	logger := log.NewDebugLogger()

	// Default values
	values := MockedValues{
		useRealAPIs: false,
		services: storageapi.Services{
			{ID: "encryption", URL: "https://encryption.mocked.transport.http"},
			{ID: "scheduler", URL: "https://scheduler.mocked.transport.http"},
			{ID: "queue", URL: "https://queue.mocked.transport.http"},
			{ID: "sandboxes", URL: "https://sandboxes.mocked.transport.http"},
		},
		features:       storageapi.Features{"FeatureA", "FeatureB"},
		components:     testapi.MockedComponents(),
		storageAPIHost: "mocked.transport.http",
		storageAPIToken: storageapi.Token{
			ID:       "token-12345-id",
			Token:    "my-secret",
			IsMaster: true,
			Owner: storageapi.TokenOwner{
				ID:       12345,
				Name:     "Project 12345",
				Features: storageapi.Features{"my-feature"},
			},
		},
		storageAPITokenMockedResponseTimes: 1,
	}

	// Apply options
	for _, opt := range opts {
		opt(&values)
	}

	// Mock APIs
	httpClient, mockedHTTPTransport := client.NewMockedClient()
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/", values.storageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, &storageapi.IndexComponents{
			Index: storageapi.Index{Services: values.services, Features: values.features}, Components: values.components,
		}).Once(),
	)

	// Mocked token verification
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/tokens/verify", values.storageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, values.storageAPIToken).Times(values.storageAPITokenMockedResponseTimes),
	)

	// Create base, public and project dependencies
	baseDeps := newBaseDeps(envs, nil, logger, httpClient)
	publicDeps, err := newPublicDeps(ctx, baseDeps, values.storageAPIHost)
	if err != nil {
		panic(err)
	}
	projectDeps, err := newProjectDeps(baseDeps, publicDeps, values.storageAPIToken)
	if err != nil {
		panic(err)
	}

	// Use real APIs
	if values.useRealAPIs {
		publicDeps.storageAPIClient = values.storageAPIClient
		projectDeps.storageAPIClient = values.storageAPIClient
		publicDeps.encryptionAPIClient = values.encryptionAPIClient
		projectDeps.schedulerAPIClient = values.schedulerAPIClient
		mockedHTTPTransport = nil
	}

	// Clear logs
	logger.Truncate()

	return &mocked{
		t:                   t,
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
	if v.mockedHTTPTransport == nil {
		panic(errors.Errorf(`mocked dependencies have been created WithTestProject(...), there is no mocked HTTP transport`))
	}
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
