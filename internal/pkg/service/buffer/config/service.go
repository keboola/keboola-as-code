package config

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// ServiceConfig is a common config of the Buffer ServiceConfig.
type ServiceConfig struct {
	DebugLog           bool                   `mapstructure:"debug-log" usage:"Enable debug log level."`
	DebugEtcd          bool                   `mapstructure:"debug-etcd" usage:"Enable logging of each etcd KV operation as a debug message."`
	DebugHTTP          bool                   `mapstructure:"debug-http" usage:"Log HTTP client request and response bodies."`
	DatadogEnabled     bool                   `mapstructure:"datadog-enabled" usage:"Enable Datadog telemetry integration."`
	DatadogDebug       bool                   `mapstructure:"datadog-debug" usage:"Enable Datadog debug logs."`
	CPUProfFilePath    string                 `mapstructure:"cpu-profile" usage:"Write cpu profile to the file."`
	StorageAPIHost     string                 `mapstructure:"storage-api-host" usage:"Host of the Storage API."`
	Etcd               etcdclient.Credentials `mapstructure:",squash" usage:"etcd client credentials."`
	EtcdConnectTimeout time.Duration          `mapstructure:"etcd-connect-timeout" usage:"etcd connect timeout."`
}

func newServiceConfig() ServiceConfig {
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
		EtcdConnectTimeout: 30 * time.Second, // longer default timeout, the etcd could be started at the same time as the API/Worker
	}
}

func (c *ServiceConfig) Normalize() {
	c.StorageAPIHost = strhelper.NormalizeHost(c.StorageAPIHost)
}

func (c *ServiceConfig) Validate() error {
	errs := errors.NewMultiError()
	if c.StorageAPIHost == "" {
		errs.Append(errors.New(`storage API host must be set`))
	}
	if c.Etcd.Endpoint == "" {
		errs.Append(errors.New(`etcd endpoint must be set`))
	}
	if c.Etcd.Namespace == "" {
		errs.Append(errors.New(`etcd namespace must be set`))
	}
	if c.EtcdConnectTimeout <= 0 {
		errs.Append(errors.Errorf(`etcd connect timeout must be positive time.Duration, found "%v"`, c.EtcdConnectTimeout))
	}
	return errs.ErrorOrNil()
}
