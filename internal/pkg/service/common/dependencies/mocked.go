package dependencies

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
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
	options             *options.Options
	mockedHTTPTransport *httpmock.MockTransport
	proc                *servicectx.Process
	requestHeader       http.Header
	etcdClient          *etcd.Client
}

type MockedConfig struct {
	ctx          context.Context
	envs         *env.Map
	clock        clock.Clock
	loggerPrefix string
	debugLogger  log.DebugLogger
	procOpts     []servicectx.Option

	etcdEndpoint  string
	etcdUsername  string
	etcdPassword  string
	etcdNamespace string

	requestHeader http.Header

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

func WithCtx(v context.Context) MockedOption {
	return func(c *MockedConfig) {
		c.ctx = v
	}
}

func WithEnvs(v *env.Map) MockedOption {
	return func(c *MockedConfig) {
		c.envs = v
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

func WithEtcdEndpoint(v string) MockedOption {
	return func(c *MockedConfig) {
		c.etcdEndpoint = v
	}
}

func WithEtcdUsername(v string) MockedOption {
	return func(c *MockedConfig) {
		c.etcdUsername = v
	}
}

func WithEtcdPassword(v string) MockedOption {
	return func(c *MockedConfig) {
		c.etcdPassword = v
	}
}

func WithEtcdNamespace(v string) MockedOption {
	return func(c *MockedConfig) {
		c.etcdNamespace = v
	}
}

func WithRequestHeader(v http.Header) MockedOption {
	return func(c *MockedConfig) {
		c.requestHeader = v
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

func NewMockedDeps(t *testing.T, opts ...MockedOption) Mocked {
	t.Helper()

	osEnvs, err := env.FromOs()
	if err != nil {
		t.Fatalf("cannot get envs: %s", err)
	}

	// Default values
	c := MockedConfig{
		ctx:           context.Background(),
		envs:          env.Empty(),
		clock:         clock.New(),
		etcdEndpoint:  osEnvs.Get("UNIT_ETCD_ENDPOINT"),
		etcdUsername:  osEnvs.Get("UNIT_ETCD_USERNAME"),
		etcdPassword:  osEnvs.Get("UNIT_ETCD_PASSWORD"),
		etcdNamespace: etcdhelper.NamespaceForTest(),
		requestHeader: make(http.Header),
		useRealAPIs:   false,
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
		opt(&c)
	}

	// Default logger
	if c.debugLogger == nil {
		c.debugLogger = log.NewDebugLogger()
	}

	var logger log.Logger
	if c.loggerPrefix != "" {
		logger = c.debugLogger.AddPrefix(c.loggerPrefix)
	} else {
		logger = c.debugLogger
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
		fmt.Sprintf("%s/v2/storage/", c.storageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Index: keboola.Index{Services: c.services, Features: c.features}, Components: c.components,
		}).Once(),
	)
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/?exclude=components", c.storageAPIHost),
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Index: keboola.Index{Services: c.services, Features: c.features}, Components: keboola.Components{},
		}),
	)

	// Mocked token verification
	verificationResponder := httpmock.NewJsonResponderOrPanic(200, c.storageAPIToken)
	if !c.multipleTokenVerification {
		verificationResponder = verificationResponder.Times(1)
	}
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/tokens/verify", c.storageAPIHost),
		verificationResponder,
	)

	// Create base, public and project dependencies
	baseDeps := newBaseDeps(c.envs, logger, telemetry.NewNopTelemetry(), c.clock, httpClient)
	publicDeps, err := newPublicDeps(c.ctx, baseDeps, c.storageAPIHost, WithPreloadComponents(true))
	if err != nil {
		panic(err)
	}
	projectDeps, err := newProjectDeps(c.ctx, baseDeps, publicDeps, c.storageAPIToken)
	if err != nil {
		panic(err)
	}

	// Use real APIs
	if c.useRealAPIs {
		publicDeps.keboolaPublicAPI = c.keboolaProjectAPI
		projectDeps.keboolaProjectAPI = c.keboolaProjectAPI
		mockedHTTPTransport = nil
		baseDeps.httpClient = client.NewTestClient()
	}

	// Clear logs
	c.debugLogger.Truncate()

	// Create service process context
	c.procOpts = append([]servicectx.Option{servicectx.WithLogger(logger)}, c.procOpts...)

	return &mocked{
		config:              c,
		t:                   t,
		base:                baseDeps,
		public:              publicDeps,
		project:             projectDeps,
		options:             options.New(),
		mockedHTTPTransport: mockedHTTPTransport,
		requestHeader:       c.requestHeader,
	}
}

func (v *mocked) EnvsMutable() *env.Map {
	return v.config.envs
}

func (v *mocked) Options() *options.Options {
	return v.options
}

func (v *mocked) DebugLogger() log.DebugLogger {
	return v.config.debugLogger
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
		if os.Getenv("UNIT_ETCD_ENABLED") == "false" { // nolint:forbidigo
			v.t.Skipf("etcd test is disabled by UNIT_ETCD_ENABLED=false")
		}
		v.etcdClient = etcdhelper.ClientForTestFrom(
			v.t,
			v.config.etcdEndpoint,
			v.config.etcdUsername,
			v.config.etcdPassword,
			v.config.etcdNamespace,
		)
	}
	return v.etcdClient
}

func (v *mocked) EtcdSerde() *serde.Serde {
	return serde.NewJSON(serde.NoValidation)
}

func (v *mocked) KeboolaProjectAPI() *keboola.API {
	return v.project.keboolaProjectAPI
}
