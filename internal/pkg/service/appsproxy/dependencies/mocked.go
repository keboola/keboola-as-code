package dependencies

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/transport/dns/dnsmock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// mocked implements Mocked interface.
type mocked struct {
	dependencies.Mocked
	config    config.Config
	dnsServer *dnsmock.Server
}

func (v *mocked) TestConfig() config.Config {
	return v.config
}

func (v *mocked) TestDNSServer() *dnsmock.Server {
	return v.dnsServer
}

func NewMockedServiceScope(tb testing.TB, ctx context.Context, cfg config.Config, opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	tb.Helper()

	commonMock := dependencies.NewMocked(tb, ctx, opts...)

	// Fill in missing fields
	if cfg.API.PublicURL == nil {
		var err error
		cfg.API.PublicURL, err = url.Parse("https://hub.keboola.local")
		require.NoError(tb, err)
	}
	if cfg.CookieSecretSalt == "" {
		cfg.CookieSecretSalt = "foo"
	}
	if cfg.CsrfTokenSalt == "" {
		cfg.CsrfTokenSalt = "bar"
	}
	if cfg.SandboxesAPI.URL == "" {
		cfg.SandboxesAPI.URL = "http://sandboxes-service-api.default.svc.cluster.local"
	}
	if cfg.SandboxesAPI.Token == "" {
		cfg.SandboxesAPI.Token = "my-token"
	}

	var dnsServer *dnsmock.Server
	if cfg.DNSServer == "" {
		dnsServer = dnsmock.New()
		require.NoError(tb, dnsServer.Start())
		tb.Cleanup(func() {
			_ = dnsServer.Shutdown()
		})

		cfg.DNSServer = dnsServer.Addr()
	}

	// Validate config
	require.NoError(tb, configmap.ValidateAndNormalize(&cfg))

	mock := &mocked{Mocked: commonMock, config: cfg, dnsServer: dnsServer}

	scope, err := newServiceScope(ctx, mock, cfg)
	require.NoError(tb, err)

	mock.DebugLogger().Truncate()
	if !commonMock.UseRealAPIs() {
		mock.MockedHTTPTransport().Reset()
	}
	return scope, mock
}
