package dependencies

import (
	"context"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type TestContainer struct {
	*CommonContainer
	ctx                         context.Context
	logger                      log.DebugLogger
	envs                        *env.Map
	fs                          filesystem.Fs
	options                     *options.Options
	apiVerboseLogs              bool
	httpTraceFactory            client.TraceFactory
	storageApiHost              string
	storageApiToken             string
	templateRepositoryFs        filesystem.Fs
	project                     *project.Project
	mockedStorageApi            *client.Client
	mockedStorageApiTransport   *httpmock.MockTransport
	mockedSchedulerApi          *client.Client
	mockedSchedulerApiTransport *httpmock.MockTransport
	mockedComponents            *model.ComponentsMap
}

var testTransport = client.DefaultTransport() // nolint:gochecknoglobals

func NewTestContainer() *TestContainer {
	ctx := context.Background()
	c := &TestContainer{ctx: ctx}
	c.CommonContainer = NewCommonContainer(ctx, c)
	c.logger = log.NewDebugLogger()
	c.envs = env.Empty()
	c.fs = testfs.NewMemoryFsWithLogger(c.logger)
	c.options = options.New()
	c.SetStorageApiHost(`storage.foo.bar`)
	c.SetStorageApiToken(`my-secret`)
	return c
}

// InitFromTestProject init test dependencies from testing project.
func (v *TestContainer) InitFromTestProject(project *testproject.Project) {
	v.SetStorageApiHost(project.StorageAPIHost())
	v.SetStorageApiToken(project.StorageAPIToken().Token)
	v.SetStorageApi(project.StorageAPIClient(), project.StorageAPIToken())
	v.SetSchedulerApi(project.SchedulerAPIClient())
	v.SetEncryptionApi(project.EncryptionAPIClient())
}

func (v *TestContainer) Ctx() context.Context {
	return v.ctx
}

func (v *TestContainer) Logger() log.Logger {
	return v.logger
}

func (v *TestContainer) DebugLogger() log.DebugLogger {
	return v.logger
}

func (v *TestContainer) Fs() filesystem.Fs {
	return v.fs
}

func (v *TestContainer) FileLoader() filesystem.FileLoader {
	return v.fs.FileLoader()
}

func (v *TestContainer) Envs() *env.Map {
	return v.envs
}

func (v *TestContainer) SetFs(fs filesystem.Fs) {
	v.fs = fs
}

func (v *TestContainer) Options() *options.Options {
	return v.options
}

func (v *TestContainer) HttpClientVerboseLogs() bool {
	return v.apiVerboseLogs
}

func (v *TestContainer) HttpTransport() http.RoundTripper {
	return testTransport
}

func (v *TestContainer) HttpTraceFactory() client.TraceFactory {
	return v.httpTraceFactory
}

func (v *TestContainer) SetApiVerboseLogs(value bool) {
	v.apiVerboseLogs = value
}

func (v *TestContainer) StorageApiHost() (string, error) {
	if v.storageApiHost == `` {
		return ``, fmt.Errorf(`dependencies: Storage API host is not set in test dependencies`)
	}
	return v.storageApiHost, nil
}

func (v *TestContainer) SetStorageApiHost(host string) {
	v.storageApiHost = host
}

func (v *TestContainer) StorageApiToken() (string, error) {
	if v.storageApiToken == `` {
		return ``, fmt.Errorf(`dependencies: Storage API host is not set in test dependencies`)
	}
	return v.storageApiToken, nil
}

func (v *TestContainer) SetStorageApiToken(token string) {
	v.storageApiToken = token
}

func (v *TestContainer) SetTemplateRepositoryFs(fs filesystem.Fs) {
	v.templateRepositoryFs = fs
}

func (v *TestContainer) TemplateRepositoryFs(_ model.TemplateRepository, _ model.TemplateRef) (filesystem.Fs, error) {
	if v.templateRepositoryFs == nil {
		return nil, fmt.Errorf("ttemplateRepositoryFs is not set in test dependencies container")
	}
	return v.templateRepositoryFs, nil
}

func (v *TestContainer) SetCtx(ctx context.Context) {
	v.ctx = ctx
}

func (v *TestContainer) SetStorageApi(client client.Client, token *storageapi.Token) {
	v.storageApi.Set(clientWithToken{Client: client, Token: token})
}

func (v *TestContainer) SetEncryptionApi(client client.Client) {
	v.encryptionApi.Set(client)
}

func (v *TestContainer) SetSchedulerApi(client client.Client) {
	v.schedulerApi.Set(client)
}

func (v *TestContainer) EventSender(sender event.Sender) {
	v.eventSender.Set(sender)
}

func (v *TestContainer) UseMockedStorageApi() (client.Client, *httpmock.MockTransport) {
	if v.mockedStorageApi == nil {
		c, transport := client.NewMockedClient()
		v.mockedStorageApi, v.mockedStorageApiTransport = &c, transport
	}
	v.SetStorageApi(*v.mockedStorageApi, nil)
	v.UseMockedComponents()
	return *v.mockedStorageApi, v.mockedStorageApiTransport
}

func (v *TestContainer) UseMockedSchedulerApi() (client.Client, *httpmock.MockTransport) {
	if v.mockedSchedulerApi == nil {
		c, transport := client.NewMockedClient()
		v.mockedSchedulerApi, v.mockedSchedulerApiTransport = &c, transport
	}
	v.SetSchedulerApi(*v.mockedSchedulerApi)
	return *v.mockedSchedulerApi, v.mockedSchedulerApiTransport
}

func (v *TestContainer) UseMockedComponents() model.ComponentsMap {
	if v.mockedComponents == nil {
		components := testapi.MockedComponentsMap()
		v.mockedComponents = &components
		v.components.Set(components)
	}
	return *v.mockedComponents
}

// EmptyState without mappers. Useful for mappers unit tests.
func (v *TestContainer) EmptyState() *state.State {
	// Enable mocked APIs
	v.UseMockedSchedulerApi()
	v.UseMockedStorageApi()

	// Create mocked state
	mockedState, err := state.New(NewObjectsContainer(v.Fs(), fixtures.NewManifest()), v)
	if err != nil {
		panic(err)
	}

	return mockedState
}

func (v *TestContainer) LocalProject(ignoreErrors bool) (*project.Project, error) {
	if v.project == nil {
		if p, err := project.New(v.fs, ignoreErrors, v); err != nil {
			return nil, err
		} else {
			v.project = p
		}
	}
	return v.project, nil
}

func (v *TestContainer) SetLocalProject(project *project.Project) {
	v.project = project
}

func (v *TestContainer) LocalProjectState(o loadState.Options) (*project.State, error) {
	prj, err := v.LocalProject(o.IgnoreInvalidLocalState)
	if err != nil {
		return nil, err
	}
	return prj.LoadState(o)
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
