package config

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix = "STREAM_"
)

// Config of the Stream services.
type Config struct {
	DebugLog        bool              `configKey:"debugLog"  configUsage:"Enable logging at DEBUG level."`
	DebugHTTPClient bool              `configKey:"debugHTTPClient" configUsage:"Log HTTP client requests and responses as debug messages."`
	CPUProfFilePath string            `configKey:"cpuProfilePath" configUsage:"Path where CPU profile is saved."`
	NodeID          string            `configKey:"nodeID" configUsage:"Unique ID of the node in the cluster." validate:"required"`
	StorageAPIHost  string            `configKey:"storageAPIHost" configUsage:"Storage API host." validate:"required,hostname"`
	Datadog         datadog.Config    `configKey:"datadog"`
	Etcd            etcdclient.Config `configKey:"etcd"`
	Metrics         prometheus.Config `configKey:"metrics"`
	API             API               `configKey:"api"`
	Source          source.Config     `configKey:"source"`
	Sink            sink.Config       `configKey:"sink"`
	Storage         storage.Config    `configKey:"storage"`
}

type API struct {
	Listen    string   `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	PublicURL *url.URL `configKey:"publicURL" configUsage:"Public URL of the configuration HTTP API for link generation."  validate:"required"`
}

func New() Config {
	return Config{
		DebugLog:        false,
		DebugHTTPClient: false,
		CPUProfFilePath: "",
		NodeID:          "",
		StorageAPIHost:  "",
		Datadog:         datadog.NewConfig(),
		Etcd:            etcdclient.NewConfig(),
		Metrics:         prometheus.NewConfig(),
		API:             API{Listen: "0.0.0.0:8000", PublicURL: &url.URL{Scheme: "http", Host: "localhost:8000"}},
		Source:          source.NewConfig(),
		Sink:            sink.NewConfig(),
		Storage:         storage.NewConfig(),
	}
}

// GenerateAndBind generates flags and then bind flags, ENVs and config files to target configuration structures.
func GenerateAndBind(args []string, envs env.Provider) (Config, error) {
	cfg := New()
	err := configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		Args:                   args,
		EnvNaming:              env.NewNamingConvention(EnvPrefix),
		Envs:                   envs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}, &cfg)
	return cfg, err
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
