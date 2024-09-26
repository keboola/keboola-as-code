package config

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/pprof"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Config of the Stream services.
type Config struct {
	DebugLog        bool                `configKey:"debugLog"  configUsage:"Enable logging at DEBUG level."`
	DebugHTTPClient bool                `configKey:"debugHTTPClient" configUsage:"Log HTTP client requests and responses as debug messages."`
	NodeID          string              `configKey:"nodeID" configUsage:"Unique ID of the node in the cluster." validate:"required"`
	Hostname        string              `configKey:"hostname" configUsage:"Hostname for communication between nodes." validate:"required"`
	StorageAPIHost  string              `configKey:"storageApiHost" configUsage:"Storage API host." validate:"required,hostname"`
	PProf           pprof.Config        `configKey:"pprof"`
	Datadog         datadog.Config      `configKey:"datadog"`
	Etcd            etcdclient.Config   `configKey:"etcd"`
	Metrics         prometheus.Config   `configKey:"metrics"`
	API             API                 `configKey:"api"`
	Distribution    distribution.Config `configKey:"distribution"`
	Source          source.Config       `configKey:"source"`
	Sink            sink.Config         `configKey:"sink"`
	Storage         storage.Config      `configKey:"storage"`
}

type Patch struct {
	Source  *source.ConfigPatch  `json:"source,omitempty"`
	Sink    *sink.ConfigPatch    `json:"sink,omitempty"`
	Storage *storage.ConfigPatch `json:"storage,omitempty"`
}

type API struct {
	Listen    string          `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	PublicURL *url.URL        `configKey:"publicUrl" configUsage:"Public URL of the configuration HTTP API for link generation." validate:"required"`
	Task      task.NodeConfig `configKey:"task" configUsage:"Background tasks configuration." validate:"required"`
}

func New() Config {
	return Config{
		DebugLog:        false,
		DebugHTTPClient: false,
		NodeID:          "",
		StorageAPIHost:  "",
		PProf:           pprof.NewConfig(),
		Datadog:         datadog.NewConfig(),
		Etcd:            etcdclient.NewConfig(),
		Metrics:         prometheus.NewConfig(),
		API: API{
			Listen:    "0.0.0.0:8000",
			PublicURL: &url.URL{Scheme: "http", Host: "localhost:8000"},
			Task:      task.NewNodeConfig(),
		},
		Distribution: distribution.NewConfig(),
		Source:       source.NewConfig(),
		Sink:         sink.NewConfig(),
		Storage:      storage.NewConfig(),
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
