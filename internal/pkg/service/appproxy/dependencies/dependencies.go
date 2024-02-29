// Package dependencies provides dependencies for App Proxy.
//
// # Dependency Containers
//
// This package extends common dependencies from [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies].
//
// Following dependencies containers are implemented:
//   - [ServiceScope] long-lived dependencies that exist during the entire run of the proxy server.
//
// Dependency containers creation:
//   - [ServiceScope] is created at startup in main.go.
//
// The package also provides mocked dependency implementations for tests:
//   - [NewMockedServiceScope]
package dependencies

import (
	"context"
	"io"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// ServiceScope interface provides dependencies for App Proxy server.
// The container exists during the entire run of the App Proxy server.
type ServiceScope interface {
	dependencies.BaseScope
	Config() config.Config
	Loader() appconfig.Loader
}

const (
	userAgent = "keboola-app-proxy"
)

// serviceScope implements APIScope interface.
type serviceScope struct {
	parentScopes
	config config.Config
	loader appconfig.Loader
}

type parentScopes interface {
	dependencies.BaseScope
}

type parentScopesImpl struct {
	dependencies.BaseScope
}

func NewServiceScope(
	ctx context.Context,
	cfg config.Config,
	proc *servicectx.Process,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
) (v ServiceScope, err error) {
	parentSc, err := newParentScopes(ctx, cfg, proc, logger, tel, stdout, stderr)
	if err != nil {
		return nil, err
	}
	return newServiceScope(ctx, parentSc, cfg)
}

func newParentScopes(
	ctx context.Context,
	cfg config.Config,
	proc *servicectx.Process,
	logger log.Logger,
	tel telemetry.Telemetry,
	stdout io.Writer,
	stderr io.Writer,
) (v parentScopes, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.appproxy.dependencies.newParentScopes")
	defer span.End(&err)

	httpClient := httpclient.New(
		httpclient.WithTelemetry(tel),
		httpclient.WithUserAgent(userAgent),
		func(c *httpclient.Config) {
			if cfg.DebugLog {
				httpclient.WithDebugOutput(stdout)(c)
			}
			if cfg.DebugHTTPClient {
				httpclient.WithDumpOutput(stdout)(c)
			}
		},
	)

	d := &parentScopesImpl{}

	d.BaseScope = dependencies.NewBaseScope(ctx, logger, tel, stdout, stderr, clock.New(), proc, httpClient)

	return d, nil
}

func newServiceScope(ctx context.Context, parentScp parentScopes, cfg config.Config) (v *serviceScope, err error) {
	ctx, span := parentScp.Telemetry().Tracer().Start(ctx, "keboola.go.appproxy.dependencies.newServiceScope")
	defer span.End(&err)

	d := &serviceScope{}
	d.parentScopes = parentScp
	d.config = cfg
	d.loader = appconfig.NewSandboxesAPILoader(parentScp.Logger(), parentScp.Clock(), cfg.SandboxesAPIURL)

	return d, nil
}

func (v *serviceScope) Config() config.Config {
	return v.config
}

func (v *serviceScope) Loader() appconfig.Loader {
	return v.loader
}
