package config

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Config of the App Proxy.
// See "cliconfig" package for more information.
type Config struct {
	DebugLog         bool              `configKey:"debugLog" configUsage:"Enable debug log level."`
	DebugHTTPClient  bool              `configKey:"debugHTTPClient" configUsage:"Log HTTP client requests and responses as debug messages."`
	CPUProfFilePath  string            `configKey:"cpuProfilePath" configUsage:"Path where CPU profile is saved."`
	Datadog          datadog.Config    `configKey:"datadog"`
	Metrics          prometheus.Config `configKey:"metrics"`
	API              API               `configKey:"api"`
	CookieSecretSalt string            `configKey:"cookieSecretSalt" configUsage:"Cookie secret needed by OAuth 2 Proxy." validate:"required" sensitive:"true"`
	SandboxesAPI     SandboxesAPI      `configKey:"sandboxesAPI"`
}

type API struct {
	Listen    string   `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	PublicURL *url.URL `configKey:"publicURL" configUsage:"Public URL of the configuration HTTP API for link generation." validate:"required"`
}

type SandboxesAPI struct {
	URL   string `configKey:"url" configUsage:"Sandboxes API url." validate:"required"`
	Token string `configKey:"token" configUsage:"Sandboxes API token." validate:"required" sensitive:"true"`
}

func New() Config {
	return Config{
		DebugLog:        false,
		DebugHTTPClient: false,
		CPUProfFilePath: "",
		Datadog:         datadog.NewConfig(),
		Metrics:         prometheus.NewConfig(),
		API: API{
			Listen: "0.0.0.0:8000",
			PublicURL: &url.URL{
				Scheme: "http",
				Host:   "localhost:8000",
			},
		},
	}
}

func (c *Config) Normalize() {
}

func (c *Config) Validate() error {
	errs := errors.NewMultiError()
	return errs.ErrorOrNil()
}

func (c *API) Normalize() {
	if c.PublicURL != nil {
		c.PublicURL.Host = strhelper.NormalizeHost(c.PublicURL.Host)
		if c.PublicURL.Scheme == "" {
			c.PublicURL.Scheme = "https"
		}
	}
}

func (c *API) Validate() error {
	errs := errors.NewMultiError()
	if c.PublicURL == nil || c.PublicURL.String() == "" {
		errs.Append(errors.New("public address is not set"))
	}
	return errs.ErrorOrNil()
}
