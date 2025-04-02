package dependencies

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/require"
	etcdPkg "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
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

// mocked dependencies container implements Mocked interface.
type mocked struct {
	*baseScope
	*publicScope
	*projectScope
	*requestInfo
	*etcdClientScope
	t                   testing.TB
	config              *MockedConfig
	mockedHTTPTransport *httpmock.MockTransport
	testEtcdClient      *etcdPkg.Client
}

type MockedConfig struct {
	enableEtcdClient bool

	clock       clockwork.Clock
	telemetry   telemetry.ForTest
	debugLogger log.DebugLogger
	procOpts    []servicectx.Option

	etcdConfig   etcdclient.Config
	etcdDebugLog bool

	dnsPort int

	stdout io.Writer
	stderr io.Writer

	services                  keboola.Services
	features                  keboola.Features
	components                keboola.Components
	storageAPIHost            string
	storageAPIToken           keboola.Token
	multipleTokenVerification bool

	useRealAPIs       bool
	useRealHTTPClient bool
	keboolaProjectAPI *keboola.AuthorizedAPI
}

type MockedOption func(c *MockedConfig)

func WithEnabledEtcdClient() MockedOption {
	return func(c *MockedConfig) {
		c.enableEtcdClient = true
	}
}

func WithSnowflakeBackend() MockedOption {
	return func(c *MockedConfig) {
		c.storageAPIToken.Owner.HasSnowflake = true
	}
}

func WithBigQueryBackend() MockedOption {
	return func(c *MockedConfig) {
		c.storageAPIToken.Owner.HasBigquery = true
	}
}

func WithClock(v clockwork.Clock) MockedOption {
	return func(c *MockedConfig) {
		c.clock = v
	}
}

func WithDebugLogger(v log.DebugLogger) MockedOption {
	return func(c *MockedConfig) {
		c.debugLogger = v
	}
}

func WithStdout(v io.Writer) MockedOption {
	return func(c *MockedConfig) {
		c.stdout = v
	}
}

func WithStderr(v io.Writer) MockedOption {
	return func(c *MockedConfig) {
		c.stderr = v
	}
}

func WithEtcdConfig(cfg etcdclient.Config) MockedOption {
	return func(c *MockedConfig) {
		WithEnabledEtcdClient()(c)
		c.etcdConfig = cfg
	}
}

func WithEtcdDebugLog(v bool) MockedOption {
	return func(c *MockedConfig) {
		c.etcdDebugLog = v
	}
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
		c.keboolaProjectAPI = project.ProjectAPI()
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

func WithRealHTTPClient() MockedOption {
	return func(c *MockedConfig) {
		c.useRealHTTPClient = true
	}
}

func WithMockedDNSPort(port int) MockedOption {
	return func(c *MockedConfig) {
		c.dnsPort = port
	}
}

func WithEtcdSerdeUsingJSONNumbers() MockedOption {
	return func(c *MockedConfig) {
		c.etcdConfig.JSONNumbers = true
	}
}

func newMockedConfig(tb testing.TB, opts []MockedOption) *MockedConfig {
	tb.Helper()

	cfg := &MockedConfig{
		clock:       clockwork.NewRealClock(),
		telemetry:   telemetry.NewForTest(tb),
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

	if cfg.stdout == nil {
		cfg.stdout = os.Stdout // nolint:forbidigo
	}

	if cfg.stderr == nil {
		cfg.stderr = os.Stderr // nolint:forbidigo
	}

	return cfg
}

func NewMocked(tb testing.TB, ctx context.Context, opts ...MockedOption) Mocked {
	tb.Helper()

	// Default values
	cfg := newMockedConfig(tb, opts)

	// Logger
	var logger log.Logger = cfg.debugLogger

	// Mock APIs
	httpClient, mockedHTTPTransport := defaultMockedResponses(cfg)

	// Create temporary etcd namespace, see "TmpNamespace" function for details.
	// It should be deleted at the end via t.Cleanup as the LAST thing, therefore it is initialized as the first.
	if cfg.enableEtcdClient && cfg.etcdConfig.Endpoint == "" {
		cfg.etcdConfig = etcdhelper.TmpNamespace(tb)
	}

	// Create service process, WithoutSignals - so it doesn't block Ctrl+C in tests
	cfg.procOpts = append([]servicectx.Option{servicectx.WithLogger(logger), servicectx.WithoutSignals()}, cfg.procOpts...)
	proc := servicectx.NewForTest(tb, cfg.procOpts...)

	// Create dependencies container
	var err error
	d := &mocked{config: cfg, t: tb, mockedHTTPTransport: mockedHTTPTransport}
	d.baseScope = newBaseScope(ctx, logger, cfg.telemetry, cfg.stdout, cfg.stderr, cfg.clock, proc, httpClient)
	d.publicScope, err = newPublicScope(ctx, d, cfg.storageAPIHost, WithPreloadComponents(true))
	require.NoError(tb, err)
	d.projectScope, err = newProjectScope(ctx, d, cfg.storageAPIToken)
	require.NoError(tb, err)
	d.requestInfo = newRequestInfo(&http.Request{RemoteAddr: "1.2.3.4:789", Header: make(http.Header)})

	// Use real APIs
	if cfg.useRealAPIs {
		d.httpClient = client.NewTestClient()
		d.keboolaPublicAPI = cfg.keboolaProjectAPI.PublicAPI
		d.keboolaProjectAPI = cfg.keboolaProjectAPI
		d.mockedHTTPTransport = nil
	}

	if cfg.useRealHTTPClient {
		d.httpClient = client.NewTestClient()
	}

	if cfg.enableEtcdClient {
		etcdCfg := cfg.etcdConfig
		if cfg.etcdDebugLog {
			etcdCfg.DebugLog = true
		} else {
			etcdCfg.DebugLog = etcdhelper.VerboseTestLogs()
		}

		d.etcdClientScope, err = newEtcdClientScope(ctx, d, etcdCfg)
		require.NoError(tb, err)
	}

	// Clear logs
	cfg.debugLogger.Truncate()
	return d
}

func (v *mocked) UseRealAPIs() bool {
	return v.config.useRealAPIs
}

func (v *mocked) DebugLogger() log.DebugLogger {
	return v.config.debugLogger
}

func (v *mocked) TestTelemetry() telemetry.ForTest {
	return v.config.telemetry
}

func (v *mocked) TestEtcdConfig() etcdclient.Config {
	if v.config.etcdConfig.Endpoint == "" {
		panic(errors.New("dependencies etcd client scope is not initialized"))
	}
	return v.config.etcdConfig
}

// TestEtcdClient returns an etcd client for tests, for example to check etcd state.
// This client does not log into the application logger, so tests are not affected.
func (v *mocked) TestEtcdClient() *etcdPkg.Client {
	if !v.config.enableEtcdClient {
		panic(errors.New("etcd is not enabled in the mocked dependencies"))
	}
	if v.testEtcdClient == nil {
		v.testEtcdClient = etcdhelper.ClientForTest(v.t, v.config.etcdConfig)
	}
	return v.testEtcdClient
}

func (v *mocked) MockedHTTPTransport() *httpmock.MockTransport {
	if v.mockedHTTPTransport == nil {
		panic(errors.Errorf(`mocked dependencies have been created WithTestProject(...), there is no mocked HTTP transport`))
	}
	return v.mockedHTTPTransport
}

func (v *mocked) MockedDNSPort() int {
	return v.config.dnsPort
}

func (v *mocked) MockedRequest() *http.Request {
	return v.request
}

func (v *mocked) MockedProject(fs filesystem.Fs) *projectPkg.Project {
	prj, err := projectPkg.New(context.Background(), log.NewNopLogger(), fs, env.Empty(), false)
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
	// Normalize host
	host := cfg.storageAPIHost
	if !strings.HasPrefix(host, "https://") && !strings.HasPrefix(host, "http://") {
		host = "https://" + host
	}

	httpClient, mockedHTTPTransport := client.NewMockedClient()

	httpClient = httpClient.WithRetry(client.TestingRetry())

	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/", host),
		httpmock.NewJsonResponderOrPanic(200, &keboola.IndexComponents{
			Index: keboola.Index{Services: cfg.services, Features: cfg.features}, Components: cfg.components,
		}).Once(),
	)
	mockedHTTPTransport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/v2/storage/?exclude=components", host),
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
		fmt.Sprintf("%s/v2/storage/tokens/verify", host),
		verificationResponder,
	)

	return httpClient, mockedHTTPTransport
}
