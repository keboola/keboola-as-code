package config

import (
	"net/url"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Config of the Apps Proxy.
// See "cliconfig" package for more information.
type Config struct {
	DebugLog         bool              `configKey:"debugLog" configUsage:"Enable debug log level."`
	DebugHTTPClient  bool              `configKey:"debugHTTPClient" configUsage:"Log HTTP client requests and responses as debug messages."`
	CPUProfFilePath  string            `configKey:"cpuProfilePath" configUsage:"Path where CPU profile is saved."`
	Datadog          datadog.Config    `configKey:"datadog"`
	Metrics          prometheus.Config `configKey:"metrics"`
	DNSServer        string            `configKey:"dnsServer" configUsage:"DNS server for proxy. If empty, the /etc/resolv.conf is used."`
	API              API               `configKey:"api"`
	CookieSecretSalt string            `configKey:"cookieSecretSalt" configUsage:"Cookie secret needed by OAuth 2 Proxy." validate:"required" sensitive:"true"`
	Upstream         Upstream          `configKey:"-" configUsage:"Configuration options for upstream"`
	SandboxesAPI     SandboxesAPI      `configKey:"sandboxesAPI"`
	CsrfTokenSalt    string            `configKey:"csrfTokenSalt" configUsage:"Salt used for generating CSRF tokens" validate:"required" sensitive:"true"`
}

type API struct {
	Listen    string   `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	PublicURL *url.URL `configKey:"publicUrl" configUsage:"Public URL of the configuration HTTP API for link generation." validate:"required"`
}

type SandboxesAPI struct {
	URL   string `configKey:"url" configUsage:"Sandboxes API url." validate:"required"`
	Token string `configKey:"token" configUsage:"Sandboxes API token." validate:"required" sensitive:"true"`
}

type Upstream struct {
	HTTPTimeout time.Duration `configKey:"httpTimeout" configUsage:"Timeout for HTTP request on upstream"`
	WsTimeout   time.Duration `configKey:"wsTimeout" configUsage:"Timeout for websocket request on upstream"`
}

func New() Config {
	return Config{
		DebugLog:        false,
		DebugHTTPClient: false,
		CPUProfFilePath: "",
		Datadog:         datadog.NewConfig(),
		Metrics:         prometheus.NewConfig(),
		Upstream: Upstream{
			HTTPTimeout: 30 * time.Second,
			WsTimeout:   6 * time.Hour,
		},
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
