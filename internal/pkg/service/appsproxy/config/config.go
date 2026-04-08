package config

import (
	"net/url"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/pprof"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Config of the Apps Proxy.
// See "cliconfig" package for more information.
type Config struct {
	DebugLog         bool              `configKey:"debugLog" configUsage:"Enable debug log level."`
	DebugHTTPClient  bool              `configKey:"debugHTTPClient" configUsage:"Log HTTP client requests and responses as debug messages."`
	PProf            pprof.Config      `configKey:"pprof"`
	Datadog          datadog.Config    `configKey:"datadog"`
	Metrics          prometheus.Config `configKey:"metrics"`
	API              API               `configKey:"api"`
	CookieSecretSalt string            `configKey:"cookieSecretSalt" configUsage:"Cookie secret needed by OAuth 2 Proxy." validate:"required" sensitive:"true"`
	Upstream         Upstream          `configKey:"-" configUsage:"Configuration options for upstream"`
	SandboxesAPI     SandboxesAPI      `configKey:"sandboxesAPI"`
	CsrfTokenSalt    string            `configKey:"csrfTokenSalt" configUsage:"Salt used for generating CSRF tokens" validate:"required" sensitive:"true"`
	K8s              K8s               `configKey:"k8s" configUsage:"Kubernetes configuration."`
	E2bWebhook       E2bWebhook        `configKey:"e2bWebhook"`
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

type K8s struct {
	AppsNamespace string `configKey:"appsNamespace" configUsage:"Kubernetes namespace where apps (App CRDs) run." validate:"required"`
	Kubeconfig    string `configKey:"kubeconfig" configUsage:"Path to kubeconfig file. Uses in-cluster config if empty."`
}

// E2bWebhook configures the reverse-proxy endpoint that forwards E2B sandbox
// lifecycle webhooks to the keboola-operator webhook server.
// Signature verification is handled by the operator, not by the proxy.
// When UpstreamURL is empty the endpoint is disabled.
type E2bWebhook struct {
	UpstreamURL string `configKey:"upstreamUrl" configUsage:"Operator internal webhook URL (e.g. http://keboola-operator-e2b-webhook.keboola-operator.svc.cluster.local:19200/webhook/e2b). Empty disables the endpoint."`
}

func New() Config {
	return Config{
		DebugLog:        false,
		DebugHTTPClient: false,
		PProf:           pprof.NewConfig(),
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
