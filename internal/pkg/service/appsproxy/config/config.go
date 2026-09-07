package config

import (
	"net/url"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola/management"

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
	StorageAPIURL    *url.URL          `configKey:"storageApiUrl" configUsage:"Base URL of the Keboola Storage API for this stack, used for Storage token verification (kai-preview flow). Must match the stack the proxy fronts — e.g. https://connection.eu-central-1.keboola.com for an EU stack. No default; required." validate:"required"`
	KaiPreview       KaiPreview        `configKey:"kaiPreview" configUsage:"kai-preview iframe-auth configuration."`
	K8s              K8s               `configKey:"k8s" configUsage:"Kubernetes configuration."`
	E2bWebhook       E2BWebhook        `configKey:"e2bWebhook"`
	Sessions         Sessions          `configKey:"sessions" configUsage:"End-user session tracking for data apps."`

	ConnectionServiceAccountTokenPath string `configKey:"connectionServiceAccountTokenPath" configUsage:"Path to the projected Kubernetes ServiceAccount token used to authenticate to the Keboola APIs. Read per request, so a rotated token needs no restart." validate:"required"`
}

// Sessions configures tracking of end-user sessions in data apps.
//
// Events are sent to a Keboola Stream HTTP source, which lands them in a
// Storage table. Tracking is off unless StreamURL is set, so it can be enabled
// per stack — Stream is not deployed on every stack that runs the proxy.
type Sessions struct {
	// StreamURL is the full Stream HTTP source URL including the secret,
	// e.g. https://stream-in.<suffix>/stream/<projectId>/<sourceId>/<secret>.
	// Empty disables session tracking.
	StreamURL string `configKey:"streamUrl" configUsage:"Full Stream HTTP source URL including the secret. Empty disables session tracking." validate:"omitempty,url" sensitive:"true"`
	// MaxSessionLength is an absolute cap on one session, measured from the
	// mint time embedded in its id. It exists so that a browser left open on a
	// dashboard indefinitely does not report a single session measured in
	// weeks; past it, the visit continues under a new session id.
	//
	// It must exceed the upstream websocket timeout, since a session cookie
	// issued on a handshake is sized to cover that whole connection.
	MaxSessionLength time.Duration `configKey:"maxSessionLength" configUsage:"Absolute cap on the length of one session, enforced server side." validate:"required,minDuration=1m"`
	// HeartbeatInterval throttles heartbeat events per session.
	HeartbeatInterval time.Duration `configKey:"heartbeatInterval" configUsage:"Minimum interval between heartbeat events of one session." validate:"required,minDuration=10s"`
	// IdleTimeout is how long a session survives without activity. It sizes the
	// session cookie deadline on an ordinary request, evicts the proxy's
	// in-memory state, and is the window downstream uses to close sessions
	// whose session_end event never arrived.
	//
	// A websocket handshake is the exception: that cookie is issued to cover
	// the connection's whole lifetime, because after the upgrade there is no
	// response left to carry a Set-Cookie.
	IdleTimeout time.Duration `configKey:"idleTimeout" configUsage:"Inactivity after which a session is considered ended." validate:"required,minDuration=1m"`
	QueueSize   int           `configKey:"queueSize" configUsage:"Capacity of the outgoing event queue. Events are dropped when full." validate:"required,min=1"`
	Workers     int           `configKey:"workers" configUsage:"Number of goroutines sending events to Stream." validate:"required,min=1"`
	SendTimeout time.Duration `configKey:"sendTimeout" configUsage:"Timeout of a single event request to Stream." validate:"required,minDuration=1s"`
}

// KaiPreview configures the stateless iframe-auth path for the kai-preview flow.
type KaiPreview struct {
	HandshakeSigningKey string        `configKey:"handshakeSigningKey" configUsage:"HMAC key for kai-preview handshake JWT (30-60s lifetime)." validate:"required" sensitive:"true"`
	SessionSigningKey   string        `configKey:"sessionSigningKey" configUsage:"HMAC key for kai-preview session cookie JWT." validate:"required" sensitive:"true"`
	SessionTTL          time.Duration `configKey:"sessionTTL" configUsage:"Lifetime of the kai-preview session cookie (sliding)." validate:"required,minDuration=1m"`
	AllowedOrigins      []string      `configKey:"allowedOrigins" configUsage:"Origins allowed to embed apps via kai-preview and mint handshake tokens (e.g. https://connection.keboola.com). Drives both the CORS allowlist and the bootstrap CSP frame-ancestors directive." validate:"required,min=1,dive,http_url"`
}

type API struct {
	Listen    string   `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	PublicURL *url.URL `configKey:"publicUrl" configUsage:"Public URL of the configuration HTTP API for link generation." validate:"required"`
}

type SandboxesAPI struct {
	URL string `configKey:"url" configUsage:"Sandboxes API url." validate:"required"`
}

type Upstream struct {
	HTTPTimeout time.Duration `configKey:"httpTimeout" configUsage:"Timeout for HTTP request on upstream"`
	WsTimeout   time.Duration `configKey:"wsTimeout" configUsage:"Timeout for websocket request on upstream"`
}

type K8s struct {
	AppsNamespace string `configKey:"appsNamespace" configUsage:"Kubernetes namespace where apps (App CRDs) run." validate:"required"`
	Kubeconfig    string `configKey:"kubeconfig" configUsage:"Path to kubeconfig file. Uses in-cluster config if empty."`
}

// E2BWebhook configures the reverse-proxy endpoint that forwards E2B sandbox
// lifecycle webhooks to the keboola-operator webhook server.
// Signature verification is handled by the operator, not by the proxy.
// When UpstreamURL is empty the endpoint is disabled.
type E2BWebhook struct {
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
		KaiPreview: KaiPreview{
			SessionTTL: 4 * time.Hour,
		},
		Sessions: Sessions{
			MaxSessionLength:  12 * time.Hour,
			HeartbeatInterval: 5 * time.Minute,
			IdleTimeout:       30 * time.Minute,
			QueueSize:         4096,
			Workers:           4,
			SendTimeout:       5 * time.Second,
		},
		ConnectionServiceAccountTokenPath: management.DefaultServiceAccountTokenPath,
	}
}

func (c *Config) Normalize() {
}

// Validate checks the invariants the session design depends on, which no
// per-field rule can express.
func (c *Sessions) Validate() error {
	errs := errors.NewMultiError()
	if c.MaxSessionLength <= c.IdleTimeout {
		errs.Append(errors.Errorf(
			`sessions.maxSessionLength (%s) must be longer than sessions.idleTimeout (%s), otherwise a session is capped before it can even go idle`,
			c.MaxSessionLength, c.IdleTimeout,
		))
	}
	if c.HeartbeatInterval >= c.IdleTimeout {
		errs.Append(errors.Errorf(
			`sessions.heartbeatInterval (%s) must be shorter than sessions.idleTimeout (%s), otherwise an active session looks idle between heartbeats`,
			c.HeartbeatInterval, c.IdleTimeout,
		))
	}
	return errs.ErrorOrNil()
}

func (c *KaiPreview) Normalize() {
	for i, o := range c.AllowedOrigins {
		c.AllowedOrigins[i] = strings.TrimRight(o, "/")
	}
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
