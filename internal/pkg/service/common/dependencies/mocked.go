package dependencies

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

// mocked dependencies container implements Mocked interface.
type mocked struct {
	*base
	*public
	*project
	config              MockedConfig
	t                   *testing.T
	envs                *env.Map
	options             *options.Options
	mockedHTTPTransport *httpmock.MockTransport
	proc                *servicectx.Process
	requestHeader       http.Header
	etcdClient          *etcd.Client
}

type MockedConfig struct {
	ctx          context.Context
	clock        clock.Clock
	loggerPrefix string
	logger       log.DebugLogger
	procOpts     []servicectx.Option

	etcdNamespace string

	services                  storageapi.Services
	features                  storageapi.Features
	components                storageapi.Components
	storageAPIHost            string
	storageAPIToken           storageapi.Token
	multipleTokenVerification bool

	useRealAPIs         bool
	storageAPIClient    client.Client
	encryptionAPIClient client.Client
	schedulerAPIClient  client.Client
}

type MockedOption func(c *MockedConfig)

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
		c.logger = v
	}
}

func WithLoggerPrefix(v string) MockedOption {
	return func(c *MockedConfig) {
		c.loggerPrefix = v
	}
}

func WithEtcdNamespace(v string) MockedOption {
	return func(c *MockedConfig) {
		c.etcdNamespace = v
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
		c.storageAPIHost = project.StorageAPIHost()
		c.storageAPIToken = *project.StorageAPIToken()

		c.useRealAPIs = true
		c.storageAPIClient = project.StorageAPIClient()
		c.encryptionAPIClient = project.EncryptionAPIClient()
		c.schedulerAPIClient = project.SchedulerAPIClient()
	}
}

func WithMockedServices(services storageapi.Services) MockedOption {
	return func(c *MockedConfig) {
		c.services = services
	}
}

func WithMockedFeatures(features storageapi.Features) MockedOption {
	return func(c *MockedConfig) {
		c.features = features
	}
}

func WithMockedComponents(components storageapi.Components) MockedOption {
	return func(c *MockedConfig) {
		c.components = components
	}
}

func WithMockedStorageAPIHost(host string) MockedOption {
	return func(c *MockedConfig) {
		c.storageAPIHost = host
	}
}

func WithMockedStorageAPIToken(token storageapi.Token) MockedOption {
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

func NewMockedDeps(t *testing.T, opts ...MockedOption) Mocked {
	t.Helper()
	envs := env.Empty()

	// Default values
	c := MockedConfig{
		ctx:           context.Background(),
		clock:         clock.New(),
		etcdNamespace: etcdhelper.NamespaceForTest(),
		useRealAPIs:   false,
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
		multipleTokenVerification: false,
	}

	// Apply options
	for _, opt := range opts {
		opt(&c)
	}

	// Default logger
	if c.logger == nil {
		c.logger = log.NewDebugLoggerWithPrefix(c.loggerPrefix)
	}

	// Cancel context after the test
	var cancel context.CancelFunc
	c.ctx, cancel = context.WithCancel(c.ctx)
	t.Cleanup(func() {
		cancel()
	})

	// Mock APIs
	httpClient, mockedHTTPTransport := client.NewMockedClient()
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/", c.storageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, &storageapi.IndexComponents{
			Index: storageapi.Index{Services: c.services, Features: c.features}, Components: c.components,
		}).Once(),
	)

	// Mocked token verification
	verificationResponder := httpmock.NewJsonResponderOrPanic(200, c.storageAPIToken)
	if !c.multipleTokenVerification {
		verificationResponder = verificationResponder.Times(1)
	}
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("https://%s/v2/storage/tokens/verify", c.storageAPIHost),
		verificationResponder,
	)

	// Create base, public and project dependencies
	baseDeps := newBaseDeps(envs, nil, c.logger, c.clock, httpClient)
	publicDeps, err := newPublicDeps(c.ctx, baseDeps, c.storageAPIHost, WithPreloadComponents(true))
	if err != nil {
		panic(err)
	}
	projectDeps, err := newProjectDeps(baseDeps, publicDeps, c.storageAPIToken)
	if err != nil {
		panic(err)
	}

	// Use real APIs
	if c.useRealAPIs {
		publicDeps.storageAPIClient = c.storageAPIClient
		projectDeps.storageAPIClient = c.storageAPIClient
		publicDeps.encryptionAPIClient = c.encryptionAPIClient
		projectDeps.schedulerAPIClient = c.schedulerAPIClient
		mockedHTTPTransport = nil
		baseDeps.httpClient = client.NewTestClient()
	}

	// Clear logs
	c.logger.Truncate()

	// Create service process context
	c.procOpts = append([]servicectx.Option{servicectx.WithLogger(c.logger)}, c.procOpts...)

	return &mocked{
		config:              c,
		t:                   t,
		base:                baseDeps,
		public:              publicDeps,
		project:             projectDeps,
		envs:                envs,
		options:             options.New(),
		mockedHTTPTransport: mockedHTTPTransport,
		requestHeader:       make(http.Header),
	}
}

func (v *mocked) EnvsMutable() *env.Map {
	return v.envs
}

func (v *mocked) Options() *options.Options {
	return v.options
}

func (v *mocked) DebugLogger() log.DebugLogger {
	return v.config.logger
}

func (v *mocked) MockedHTTPTransport() *httpmock.MockTransport {
	if v.mockedHTTPTransport == nil {
		panic(errors.Errorf(`mocked dependencies have been created WithTestProject(...), there is no mocked HTTP transport`))
	}
	return v.mockedHTTPTransport
}

func (v *mocked) Process() *servicectx.Process {
	if v.proc == nil {
		v.proc = servicectx.NewForTest(v.t, v.config.ctx, v.config.procOpts...)
	}
	return v.proc
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

func (v *mocked) RequestCtx() context.Context {
	return context.Background()
}

func (v *mocked) RequestID() string {
	return "my-request-id"
}

func (v *mocked) RequestHeader() http.Header {
	return v.requestHeader.Clone()
}

func (v *mocked) RequestHeaderMutable() http.Header {
	return v.requestHeader
}

func (v *mocked) RequestClientIP() net.IP {
	return net.ParseIP("1.2.3.4")
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

func (v *mocked) EtcdClient() *etcd.Client {
	if v.etcdClient == nil {
		v.etcdClient = etcdhelper.ClientForTestWithNamespace(v.t, v.config.etcdNamespace)
	}
	return v.etcdClient
}
