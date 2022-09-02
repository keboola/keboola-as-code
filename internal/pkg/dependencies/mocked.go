package dependencies

import (
	"context"
	"net/http"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

// mocked dependencies container implements Mocked interface.
type mocked struct {
	*base
	*public
	*project
	envs                *env.Map
	options             *options.Options
	debugLogger         log.DebugLogger
	mockedHttpTransport *httpmock.MockTransport
}

func NewMockedDeps() Mocked {
	ctx := context.Background()
	envs := env.Empty()
	logger := log.NewDebugLogger()
	httpClient, mockedHttpTransport := client.NewMockedClient()

	// Mocked Storage API host and token
	host := "mocked.transport.http"
	token := storageapi.Token{
		ID:       "token-12345-id",
		Token:    "my-secret",
		IsMaster: true,
		Owner: storageapi.TokenOwner{
			ID:       12345,
			Name:     "Project 12345",
			Features: storageapi.Features{"my-feature"},
		},
	}

	// Mock API index
	mockedHttpTransport.RegisterResponder(
		http.MethodGet,
		"https://mocked.transport.http/v2/storage/",
		httpmock.NewJsonResponderOrPanic(200, &storageapi.IndexComponents{
			Index: storageapi.Index{
				Services: storageapi.Services{
					{ID: "encryption", URL: "https://encryption.mocked.transport.http"},
					{ID: "scheduler", URL: "https://scheduler.mocked.transport.http"},
				},
				Features: storageapi.Features{"FeatureA", "FeatureB"},
			},
			Components: testapi.MockedComponents(),
		}),
	)

	// Mocked token verification
	mockedHttpTransport.RegisterResponder(
		http.MethodGet,
		"https://mocked.transport.http/v2/storage/tokens/verify",
		httpmock.NewJsonResponderOrPanic(200, token),
	)

	// Create base, public and project dependencies
	baseDeps := newBaseDeps(envs, logger, httpClient)
	publicDeps, err := newPublicDeps(ctx, baseDeps, host)
	if err != nil {
		panic(err)
	}
	projectDeps, err := newProjectDeps(baseDeps, publicDeps, token)
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
		debugLogger:         logger,
		mockedHttpTransport: mockedHttpTransport,
	}
}

// SetFromTestProject set test dependencies from a testing project.
func (v *mocked) SetFromTestProject(project *testproject.Project) {
	v.storageApiHost = project.StorageAPIHost()
	v.public.storageApiClient = project.StorageAPIClient()
	v.project.storageApiClient = project.StorageAPIClient()
	v.project.token = *project.StorageAPIToken()
	v.encryptionApiClient = project.EncryptionAPIClient()
	v.schedulerApiClient = project.SchedulerAPIClient()
	v.mockedHttpTransport = nil
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

func (v *mocked) MockedHttpTransport() *httpmock.MockTransport {
	return v.mockedHttpTransport
}

func (v *mocked) MockedProject(fs filesystem.Fs) *projectPkg.Project {
	prj, err := projectPkg.New(context.Background(), fs, false)
	if err != nil {
		panic(err)
	}
	return prj
}

func (v *mocked) MockedState() *state.State {
	s, err := state.New(NewObjectsContainer(testfs.NewMemoryFsWithLogger(v.debugLogger), fixtures.NewManifest()), v)
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
