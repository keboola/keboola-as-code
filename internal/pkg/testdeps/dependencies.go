package testdeps

import (
	"context"
	"fmt"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

type commonDeps = dependencies.TestContainer

type TestContainer struct {
	*testDependencies
	*commonDeps
	mockedStorageApi            *storageapi.Api
	mockedStorageApiTransport   *httpmock.MockTransport
	mockedSchedulerApi          *schedulerapi.Api
	mockedSchedulerApiTransport *httpmock.MockTransport
}

func New() *TestContainer {
	test := &testDependencies{}
	test.logger = log.NewDebugLogger()
	test.envs = env.Empty()
	test.fs = testfs.NewMemoryFsWithLogger(test.logger)
	test.options = options.New()
	test.SetStorageApiHost(`storage.foo.bar`)
	test.SetStorageApiToken(`my-secret`)
	common := dependencies.NewTestContainer(test, context.Background())
	all := &TestContainer{testDependencies: test, commonDeps: common}
	test._all = all
	return all
}

type testDependencies struct {
	_all            *TestContainer
	logger          log.DebugLogger
	envs            *env.Map
	fs              filesystem.Fs
	options         *options.Options
	projectId       int
	apiVerboseLogs  bool
	storageApiHost  string
	storageApiToken string
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

func (v *testDependencies) Logger() log.Logger {
	return v.logger
}

func (v *testDependencies) DebugLogger() log.DebugLogger {
	return v.logger
}

func (v *testDependencies) Fs() filesystem.Fs {
	return v.fs
}

func (v *testDependencies) FileLoader() filesystem.FileLoader {
	return v.fs.FileLoader()
}

func (v *testDependencies) Envs() *env.Map {
	return v.envs
}

func (v *testDependencies) SetFs(fs filesystem.Fs) {
	v.fs = fs
}

func (v *testDependencies) Options() *options.Options {
	return v.options
}

func (v *testDependencies) SetProjectId(projectId int) {
	v.projectId = projectId
}

func (v *testDependencies) ApiVerboseLogs() bool {
	return v.apiVerboseLogs
}

func (v *testDependencies) SetApiVerboseLogs(value bool) {
	v.apiVerboseLogs = value
}

func (v *testDependencies) StorageApiHost() (string, error) {
	if v.storageApiHost == `` {
		return ``, fmt.Errorf(`dependencies: Storage API host is not set in test dependencies`)
	}
	return v.storageApiHost, nil
}

func (v *testDependencies) SetStorageApiHost(host string) {
	v.storageApiHost = host
}

func (v *testDependencies) StorageApiToken() (string, error) {
	if v.storageApiToken == `` {
		return ``, fmt.Errorf(`dependencies: Storage API host is not set in test dependencies`)
	}
	return v.storageApiToken, nil
}

func (v *testDependencies) SetStorageApiToken(token string) {
	v.storageApiToken = token
}
