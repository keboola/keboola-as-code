package config

import (
	"net/http"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	DefaultStatisticsL2CacheTTL = 1 * time.Second
)

// ServiceConfig is a common config of the Buffer ServiceConfig.
type ServiceConfig struct {
	DebugLog             bool                   `mapstructure:"debug-log" usage:"Enable debug log level."`
	DebugEtcd            bool                   `mapstructure:"debug-etcd" usage:"Enable logging of each etcd KV operation as a debug message."`
	DebugHTTP            bool                   `mapstructure:"debug-http" usage:"Log HTTP client request and response bodies."`
	DatadogEnabled       bool                   `mapstructure:"datadog-enabled" usage:"Enable Datadog telemetry integration."`
	DatadogDebug         bool                   `mapstructure:"datadog-debug" usage:"Enable Datadog debug logs."`
	CPUProfFilePath      string                 `mapstructure:"cpu-profile" usage:"Write cpu profile to the file."`
	StorageAPIHost       string                 `mapstructure:"storage-api-host" usage:"Host of the Storage API."`
	Etcd                 etcdclient.Credentials `mapstructure:",squash" usage:"etcd client credentials."`
	EtcdConnectTimeout   time.Duration          `mapstructure:"etcd-connect-timeout" usage:"etcd connect timeout."`
	StatisticsL2CacheTTL time.Duration          `mapstructure:"statistics-l2-cache-ttl" usage:"Invalidation interval fast L2 statistics cache."`
	UploadTransport      http.RoundTripper      `json:"-"`
}

func NewServiceConfig() ServiceConfig {
	return ServiceConfig{
		DebugLog:        false,
		DebugEtcd:       false,
		DebugHTTP:       false,
		CPUProfFilePath: "",
		DatadogEnabled:  true,
		DatadogDebug:    false,
		StorageAPIHost:  "",
		Etcd: etcdclient.Credentials{
			Endpoint:  "",
			Namespace: "",
			Username:  "",
			Password:  "",
		},
		EtcdConnectTimeout:   30 * time.Second, // longer default timeout, the etcd could be started at the same time as the API/Worker
		StatisticsL2CacheTTL: DefaultStatisticsL2CacheTTL,
		UploadTransport:      nil, // use default transport
	}
}

func (c *ServiceConfig) Normalize() {
	c.StorageAPIHost = strhelper.NormalizeHost(c.StorageAPIHost)
	c.Etcd.Normalize()
}

func (c *ServiceConfig) Validate() error {
	errs := errors.NewMultiError()
	if c.StorageAPIHost == "" {
		errs.Append(errors.New(`storage API host must be set`))
	}
	if err := c.Etcd.Validate(); err != nil {
		errs.Append(err)
	}
	if c.EtcdConnectTimeout <= 0 {
		errs.Append(errors.Errorf(`etcd connect timeout must be positive time.Duration, found "%v"`, c.EtcdConnectTimeout))
	}
	if c.StatisticsL2CacheTTL <= 0 {
		errs.Append(errors.New("statistics L2 cache TTL must be a positive value"))
	}
	return errs.ErrorOrNil()
}
