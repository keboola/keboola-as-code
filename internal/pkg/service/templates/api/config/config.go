package config

import (
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/datadog"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"net/url"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix              = "TEMPLATES_"
	DefaultCleanupInterval = 1 * time.Hour
)

// Config of the Templates API.
// See "configmap" package for more information.
type Config struct {
	DebugLog             bool              `configKey:"debug-log" configUsage:"Enable debug log level."`
	DebugHTTPClient      bool              `configKey:"debugHTTPClient" configUsage:"Log HTTP client requests and responses as debug messages."`
	CPUProfFilePath      string            `configKey:"cpuProfilePath" configUsage:"Path where CPU profile is saved."`
	NodeID               string            `configKey:"nodeID" configUsage:"Unique ID of the node in the cluster." validate:"required"`
	Datadog              datadog.Config    `configKey:"datadog"`
	Etcd                 etcdclient.Config `configKey:"etcd"`
	Metrics              prometheus.Config `configKey:"metrics"`
	API                  API               `configKey:"api"`
	StorageAPIHost       string            `configKey:"storage-api-host" configUsage:"Host of the Storage API."`
	Repositories         Repositories      `configKey:"repositories" configUsage:"Default repositories, <name1>|<repo1>|<branch1>;..."`
	TasksCleanup         bool              `configKey:"tasks-cleanup-enabled" configUsage:"Enable periodical tasks cleanup functionality."`
	TasksCleanupInterval time.Duration     `configKey:"tasks-cleanup-interval" configUsage:"How often will old tasks be deleted."`
}

type API struct {
	Listen    string   `configKey:"listen" configUsage:"Listen address of the configuration HTTP API." validate:"required,hostname_port"`
	PublicURL *url.URL `configKey:"publicURL" configUsage:"Public URL of the configuration HTTP API for link generation."  validate:"required"`
}

func New() Config {
	return Config{
		DebugLog:             false,
		DebugHTTPClient:      false,
		CPUProfFilePath:      "",
		NodeID:               "",
		Datadog:              datadog.NewConfig(),
		Etcd:                 etcdclient.NewConfig(),
		Metrics:              prometheus.NewConfig(),
		API:                  API{Listen: "0.0.0.0:8000", PublicURL: &url.URL{Scheme: "http", Host: "localhost:8000"}},
		StorageAPIHost:       "",
		Repositories:         DefaultRepositories(),
		TasksCleanup:         true,
		TasksCleanupInterval: DefaultCleanupInterval,
	}
}

func Bind(args []string, envs env.Provider) (Config, error) {
	cfg := New()
	err := configmap.Bind(configmap.BindSpec{
		Args:                   args,
		EnvNaming:              env.NewNamingConvention(EnvPrefix),
		Envs:                   envs,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}, &cfg)
	return cfg, err
}

func (c *Config) Normalize() {
	c.StorageAPIHost = strhelper.NormalizeHost(c.StorageAPIHost)
}

func (c *Config) Validate() error {
	errs := errors.NewMultiError()
	if c.TasksCleanupInterval <= 0 {
		return errors.Errorf(`tasks cleanup interval must be positive time.Duration, found "%v"`, c.TasksCleanupInterval)
	}
	if c.StorageAPIHost == "" {
		errs.Append(errors.New(`storage API host must be set`))
	}
	if len(c.Repositories) == 0 {
		errs.Append(errors.New(`at least one default repository must be set`))
	}
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
