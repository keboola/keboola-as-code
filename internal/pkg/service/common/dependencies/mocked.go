package dependencies

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/require"
	etcdPkg "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

const (
	distributionGroup = "my-group"
)

// mocked dependencies container implements Mocked interface.
type mocked struct {
	*baseScope
	*publicScope
	*projectScope
	*requestInfo
	*etcdClientScope
	*taskScope
	*distributionScope
	*orchestratorScope
	t                   *testing.T
	config              *MockedConfig
	mockedHTTPTransport *httpmock.MockTransport
	testEtcdClient      *etcdPkg.Client
}

type MockedConfig struct {
	enableEtcdClient   bool
	enableTasks        bool
	enableDistribution bool
	enableOrchestrator bool

	ctx          context.Context
	clock        clock.Clock
	telemetry    telemetry.ForTest
	loggerPrefix string
	debugLogger  log.DebugLogger
	procOpts     []servicectx.Option

	etcdCredentials etcdclient.Credentials

	services                  keboola.Services
	features                  keboola.Features
	components                keboola.Components
	storageAPIHost            string
	storageAPIToken           keboola.Token
	multipleTokenVerification bool

	useRealAPIs       bool
	keboolaProjectAPI *keboola.API
}

type MockedOption func(c *MockedConfig)

func WithEnabledEtcdClient() MockedOption {
	return func(c *MockedConfig) {
		c.enableEtcdClient = true
	}
}

func WithEnabledTasks() MockedOption {
	return func(c *MockedConfig) {
		WithEnabledEtcdClient()(c)
		c.enableTasks = true
	}
}

func WithEnabledDistribution() MockedOption {
	return func(c *MockedConfig) {
		WithEnabledEtcdClient()(c)
		c.enableDistribution = true
	}
}

func WithEnabledOrchestrator() MockedOption {
	return func(c *MockedConfig) {
		WithEnabledTasks()(c)
		WithEnabledDistribution()(c)
		c.enableOrchestrator = true
	}
}

func WithCtx(v context.Context) MockedOption {
	return func(c *MockedConfig) {
		c.ctx = v
	}
}

func WithClock(v clock.Clock) MockedOption {
	return func(c *MockedConfig) {
		c.clock = v
	}
}

func WithDebugLogger(v log.DebugLogger) MockedOption {
	return func(c *MockedConfig) {
		c.debugLogger = v
	}
}

func WithLoggerPrefix(v string) MockedOption {
	return func(c *MockedConfig) {
		c.loggerPrefix = v
	}
}

func WithEtcdCredentials(credentials etcdclient.Credentials) MockedOption {
	return func(c *MockedConfig) {
		WithEnabledEtcdClient()(c)
		c.etcdCredentials = credentials
	}
}

func WithUniqueID(v string) MockedOption {
	return WithProcessOptions(servicectx.WithUniqueID(v))
}

func WithProcessOptions(opts ...servicectx.Option) MockedOption {
	return func(c *MockedConfig) {
		c.procOpts = append(c.procOpts, opts...)
	}
}

func WithTestProject(project *testproject.Project) MockedOption {
	return func(c *MockedConfig) {
		host := project.StorageAPIHost()
		if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
			host = "https://" + host
		}

		c.storageAPIHost = host
		c.storageAPIToken = *project.StorageAPIToken()

		c.useRealAPIs = true
		c.keboolaProjectAPI = project.KeboolaProjectAPI()
	}
}

func WithTelemetry(tel telemetry.ForTest) MockedOption {
	return func(c *MockedConfig) {
		c.telemetry = tel
	}
}

func WithMockedServices(services keboola.Services) MockedOption {
	return func(c *MockedConfig) {
		c.services = services
	}
}

func WithMockedFeatures(features keboola.Features) MockedOption {
	return func(c *MockedConfig) {
		c.features = features
	}
}

func WithMockedComponents(components keboola.Components) MockedOption {
	return func(c *MockedConfig) {
		c.components = components
	}
}

func WithMockedStorageAPIHost(host string) MockedOption {
	return func(c *MockedConfig) {
		c.storageAPIHost = host
	}
}

func WithMockedStorageAPIToken(token keboola.Token) MockedOption {
	return func(c *MockedConfig) {
		c.storageAPIToken = token
	}
}

// WithMultipleTokenVerification allows the mocked token verification to be called multiple times.
func WithMultipleTokenVerification(v bool) MockedOption {
	return func(c *MockedConfig) {
		c.multipleTokenVerification = v
	}
}

func newMockedConfig(t *testing.T, opts []MockedOption) *MockedConfig {
	t.Helper()

	cfg := &MockedConfig{
		ctx:         context.Background(),
		clock:       clock.New(),
		telemetry:   telemetry.NewForTest(t),
		useRealAPIs: false,
		services: keboola.Services{
			{ID: "encryption", URL: "https://encryption.mocked.transport.http"},
			{ID: "scheduler", URL: "https://scheduler.mocked.transport.http"},
			{ID: "queue", URL: "https://queue.mocked.transport.http"},
			{ID: "sandboxes", URL: "https://sandboxes.mocked.transport.http"},
		},
		features:       keboola.Features{"FeatureA", "FeatureB"},
		components:     testapi.MockedComponents(),
		storageAPIHost: "https://mocked.transport.http",
		storageAPIToken: keboola.Token{
			ID:       "token-12345-id",
			Token:    "my-secret",
			IsMaster: true,
			Owner: keboola.TokenOwner{
				ID:       12345,
				Name:     "Project 12345",
				Features: keboola.Features{"my-feature"},
			},
		},
		multipleTokenVerification: false,
	}

	// Apply options
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.debugLogger == nil {
		cfg.debugLogger = log.NewDebugLogger()
		cfg.debugLogger.ConnectTo(testhelper.VerboseStdout())
	}

	return cfg
}

func NewMocked(t *testing.T, opts ...MockedOption) Mocked {
	t.Helper()

	// Default values
	cfg := newMockedConfig(t, opts)

	// Logger
	var logger log.Logger = cfg.debugLogger
	if cfg.loggerPrefix != "" {
		logger = logger.AddPrefix(cfg.loggerPrefix)
	}

	// Cancel context after the test
	var cancel context.CancelFunc
	cfg.ctx, cancel = context.WithCancel(cfg.ctx)
	t.Cleanup(func() {
		cancel()
	})

	// Mock APIs
	httpClient, mockedHTTPTransport := defaultMockedResponses(cfg)

	// Create service process
	cfg.procOpts = append([]servicectx.Option{servicectx.WithLogger(logger)}, cfg.procOpts...)
	proc := servicectx.NewForTest(t, cfg.ctx, cfg.procOpts...)

	// Create dependencies container
	var err error
	d := &mocked{config: cfg, t: t, mockedHTTPTransport: mockedHTTPTransport}
	d.baseScope = newBaseScope(cfg.ctx, logger, cfg.telemetry, cfg.clock, proc, httpClient)
	d.publicScope, err = newPublicScope(cfg.ctx, d, cfg.storageAPIHost, WithPreloadComponents(true))
	require.NoError(t, err)
	d.projectScope, err = newProjectScope(cfg.ctx, d, cfg.storageAPIToken)
	require.NoError(t, err)
	d.requestInfo = newRequestInfo(&http.Request{RemoteAddr: "1.2.3.4:789", Header: make(http.Header)})

	// Use real APIs
	if cfg.useRealAPIs {
		d.baseScope.httpClient = client.NewTestClient()
		d.publicScope.keboolaPublicAPI = cfg.keboolaProjectAPI
		d.projectScope.keboolaProjectAPI = cfg.keboolaProjectAPI
		d.mockedHTTPTransport = nil
	}

	if cfg.enableEtcdClient {
		if cfg.etcdCredentials.Endpoint == "" {
			cfg.etcdCredentials = etcdhelper.TmpNamespace(t)
		}
		etcdOpts := []etcdclient.Option{
			etcdclient.WithDebugOpLogs(etcdhelper.VerboseTestLogs()),
		}
		d.etcdClientScope, err = newEtcdClientScope(cfg.ctx, d, cfg.etcdCredentials, etcdOpts...)
		require.NoError(t, err)
	}

	if cfg.enableTasks {
		d.taskScope, err = newTaskScope(cfg.ctx, d)
		require.NoError(t, err)
	}

	if cfg.enableDistribution {
		d.distributionScope, err = newDistributionScope(cfg.ctx, d, distributionGroup)
		require.NoError(t, err)
	}

	if cfg.enableOrchestrator {
		d.orchestratorScope = newOrchestratorScope(cfg.ctx, d)
	}

	// Clear logs
	cfg.debugLogger.Truncate()

	return d
}

func (v *mocked) DebugLogger() log.DebugLogger {
	return v.config.debugLogger
}

func (v *mocked) TestContext() context.Context {
	return v.config.ctx
}

func (v *mocked) TestTelemetry() telemetry.ForTest {
	return v.config.telemetry
}

func (v *mocked) TestEtcdCredentials() etcdclient.Credentials {
	if v.config.etcdCredentials.Endpoint == "" {
		panic(errors.New("dependencies etcd client scope is not initialized"))
	}
	return v.config.etcdCredentials
}

// TestEtcdClient returns an etcd client for tests, for example to check etcd state.
// This client does not log into the application logger, so tests are not affected.
func (v *mocked) TestEtcdClient() *etcdPkg.Client {
	if !v.config.enableEtcdClient {
		panic(errors.New("etcd is not enabled in the mocked dependencies"))
	}
	if v.testEtcdClient == nil {
		v.testEtcdClient = etcdhelper.ClientForTest(v.t, v.config.etcdCredentials)
	}
	return v.testEtcdClient
}

func (v *mocked) MockedHTTPTransport() *httpmock.MockTransport {
	if v.mockedHTTPTransport == nil {
		panic(errors.Errorf(`mocked dependencies have been created WithTestProject(...), there is no mocked HTTP transport`))
	}
	return v.mockedHTTPTransport
}

func (v *mocked) MockedRequest() *http.Request {
	return v.requestInfo.request
}

func (v *mocked) MockedProject(fs filesystem.Fs) *projectPkg.Project {
	prj, err := projectPkg.New(context.Background(), fs, false)
	if err != nil {
		panic(err)
	}
	return prj
}

func (v *mocked) MockedState() *state.State {
	s, err := state.New(context.Background(), NewObjectsContainer(aferofs.NewMemoryFs(filesystem.WithLogger(v.Logger())), fixtures.NewManifest()), v)
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

func defaultMockedResponses(cfg *MockedConfig) (client.Client, *httpmock.MockTransport) {
	httpClient, mockedHTTPTransport := client.NewMockedClient()
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/", cfg.storageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Index: keboola.Index{Services: cfg.services, Features: cfg.features}, Components: cfg.components,
		}).Once(),
	)
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/?exclude=components", cfg.storageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Index: keboola.Index{Services: cfg.services, Features: cfg.features}, Components: keboola.Components{},
		}),
	)

	// Mocked token verification
	verificationResponder := httpmock.NewJsonResponderOrPanic(200, cfg.storageAPIToken)
	if !cfg.multipleTokenVerification {
		verificationResponder = verificationResponder.Times(1)
	}
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/tokens/verify", cfg.storageAPIHost),
		verificationResponder,
	)

	return httpClient, mockedHTTPTransport
}
