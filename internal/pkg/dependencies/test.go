package dependencies

import (
	"context"
	"fmt"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi/eventsender"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

type TestContainer struct {
	*commonContainer
	logger                      log.DebugLogger
	envs                        *env.Map
	fs                          filesystem.Fs
	options                     *options.Options
	projectId                   int
	apiVerboseLogs              bool
	components                  *model.ComponentsMap
	storageApiHost              string
	storageApiToken             string
	templateRepositoryFs        filesystem.Fs
	mockedStorageApi            *storageapi.Api
	mockedStorageApiTransport   *httpmock.MockTransport
	mockedSchedulerApi          *schedulerapi.Api
	mockedSchedulerApiTransport *httpmock.MockTransport
}

func NewTestContainer() *TestContainer {
	c := &TestContainer{}
	c.commonContainer = NewCommonContainer(c, context.Background()).(*commonContainer)
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

func (v *TestContainer) Components() (*model.ComponentsMap, error) {
	if v.components == nil {
		storageApi, err := v.StorageApi()
		if err != nil {
			return nil, err
		}
		v.components = storageApi.Components()
	}
	return v.components, nil
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

func (v *TestContainer) UseMockedComponents() {
	v.components = model.NewComponentsMap(testapi.NewMockedComponentsProvider())
}

func (v *TestContainer) UseMockedStorageApi() (*storageapi.Api, *httpmock.MockTransport) {
	if v.mockedStorageApi == nil {
		v.mockedStorageApi, v.mockedStorageApiTransport = testapi.NewMockedStorageApi(v.DebugLogger())
	}

	v.SetStorageApi(v.mockedStorageApi)
	testapi.AddMockedComponents(v.mockedStorageApiTransport)
	return v.mockedStorageApi, v.mockedStorageApiTransport
}

func (v *TestContainer) UseMockedSchedulerApi() (*schedulerapi.Api, *httpmock.MockTransport) {
	if v.mockedSchedulerApi == nil {
		v.mockedSchedulerApi, v.mockedSchedulerApiTransport = testapi.NewMockedSchedulerApi(v.DebugLogger())
	}

	v.SetSchedulerApi(v.mockedSchedulerApi)
	return v.mockedSchedulerApi, v.mockedSchedulerApiTransport
}

// EmptyLocalState without mappers. Useful for mappers unit tests.
func (v *TestContainer) EmptyLocalState() *local.State {
	// Enable mocked APIs
	v.UseMockedComponents()
	v.UseMockedSchedulerApi()
	v.UseMockedStorageApi()

	// Create mocked state
	mockedState, err := local.NewState(v, v.Fs(), manifest.NewInMemory(), nil)
	if err != nil {
		panic(err)
	}

	return mockedState
}

// EmptyRemoteState without mappers. Useful for mappers unit tests.
func (v *TestContainer) EmptyRemoteState() *remote.State {
	// Enable mocked APIs
	v.UseMockedComponents()
	v.UseMockedSchedulerApi()
	v.UseMockedStorageApi()

	// Create mocked state
	mockedState, err := remote.NewState(v, state.NewIdSorter(), nil)
	if err != nil {
		panic(err)
	}

	return mockedState
}
