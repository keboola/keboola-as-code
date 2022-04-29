package dependencies

import (
	"context"
	"fmt"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi/eventsender"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
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
	projectId                   int
	apiVerboseLogs              bool
	storageApiHost              string
	storageApiToken             string
	templateRepositoryFs        filesystem.Fs
	project                     *project.Project
	mockedStorageApi            *storageapi.Api
	mockedStorageApiTransport   *httpmock.MockTransport
	mockedSchedulerApi          *schedulerapi.Api
	mockedSchedulerApiTransport *httpmock.MockTransport
}

func NewTestContainer() *TestContainer {
	c := &TestContainer{ctx: context.Background()}
	c.CommonContainer = NewCommonContainer(c)
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
	storageApi := project.StorageApi()
	v.SetProjectId(project.Id())
	v.SetStorageApiHost(storageApi.Host())
	v.SetStorageApiToken(storageApi.Token().Token)
	v.SetStorageApi(storageApi)
	v.SetSchedulerApi(project.SchedulerApi())
	v.SetEncryptionApi(project.EncryptionApi())
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

func (v *TestContainer) SetProjectId(projectId int) {
	v.projectId = projectId
}

func (v *TestContainer) ApiVerboseLogs() bool {
	return v.apiVerboseLogs
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

func (v *TestContainer) SetStorageApi(api *storageapi.Api) {
	v.storageApi = api
}

func (v *TestContainer) SetEncryptionApi(api *encryptionapi.Api) {
	v.encryptionApi = api
}

func (v *TestContainer) SetSchedulerApi(api *schedulerapi.Api) {
	v.schedulerApi = api
}

func (v *TestContainer) EventSender(sender *eventsender.Sender) {
	v.eventSender = sender
}

func (v *TestContainer) UseMockedStorageApi() (*storageapi.Api, *httpmock.MockTransport) {
	if v.mockedStorageApi == nil {
		v.mockedStorageApi, v.mockedStorageApiTransport = testapi.NewMockedStorageApi(v.DebugLogger())
	}

	v.SetStorageApi(v.mockedStorageApi)
	return v.mockedStorageApi, v.mockedStorageApiTransport
}

func (v *TestContainer) UseMockedSchedulerApi() (*schedulerapi.Api, *httpmock.MockTransport) {
	if v.mockedSchedulerApi == nil {
		v.mockedSchedulerApi, v.mockedSchedulerApiTransport = testapi.NewMockedSchedulerApi(v.DebugLogger())
	}

	v.SetSchedulerApi(v.mockedSchedulerApi)
	return v.mockedSchedulerApi, v.mockedSchedulerApiTransport
}

// EmptyState without mappers. Useful for mappers unit tests.
func (v *TestContainer) EmptyState() *state.State {
	// Enable mocked APIs
	v.UseMockedSchedulerApi()
	_, httpTransport := v.UseMockedStorageApi()
	testapi.AddMockedComponents(httpTransport)

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
