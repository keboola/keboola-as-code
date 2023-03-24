package config

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix = "TEMPLATES_API_"
)

// Config of the Templates API.
// See "cliconfig" package for more information.
type Config struct {
	Debug           bool         `mapstructure:"debug" usage:"Enable debug log level."`
	DebugHTTP       bool         `mapstructure:"debug-http" usage:"Log HTTP client request and response bodies."`
	DatadogEnabled  bool         `mapstructure:"datadog-enabled" usage:"Enable Datadog telemetry integration."`
	DatadogDebug    bool         `mapstructure:"datadog-debug" usage:"Enable Datadog debug logs."`
	CpuProfFilePath string       `mapstructure:"cpu-profile" usage:"Write cpu profile to the file."`
	ListenAddress   *url.URL     `mapstructure:"listen-address" usage:"Server listen address."`
	StorageAPIHost  string       `mapstructure:"storage-api-host" usage:"Host of the Storage API."`
	EtcdEnabled     bool         `mapstructure:"etcd-enabled" usage:"Enable etcd integration for locks."`
	EtcdEndpoint    string       `mapstructure:"etcd-endpoint" usage:"etcd endpoint."`
	EtcdNamespace   string       `mapstructure:"etcd-namespace" usage:"etcd namespace."`
	EtcdUsername    string       `mapstructure:"etcd-username" usage:"etcd username."`
	EtcdPassword    string       `mapstructure:"etcd-password" usage:"etcd password."`
	Repositories    Repositories `mapstructure:"repositories" usage:"Default repositories, <name1>|<repo1>|<branch1>;..."`
}

type Repositories []model.TemplateRepository

type Option func(c *Config)

func NewConfig() Config {
	return Config{
		Debug:           false,
		DebugHTTP:       false,
		CpuProfFilePath: "",
		DatadogEnabled:  true,
		DatadogDebug:    false,
		ListenAddress:   &url.URL{Scheme: "http", Host: "0.0.0.0:8000"},
		Repositories: []model.TemplateRepository{
			{
				Type: model.RepositoryTypeGit,
				Name: "keboola",
				URL:  "https://github.com/keboola/keboola-as-code-templates.git",
				Ref:  "main",
			},
			{
				Type: model.RepositoryTypeGit,
				Name: "keboola-beta",
				URL:  "https://github.com/keboola/keboola-as-code-templates.git",
				Ref:  "beta",
			},
			{
				Type: model.RepositoryTypeGit,
				Name: "keboola-dev",
				URL:  "https://github.com/keboola/keboola-as-code-templates.git",
				Ref:  "dev",
			},
		},
	}
}

func LoadFrom(args []string, envs env.Provider) (Config, error) {
	cfg := NewConfig()
	err := cfg.LoadFrom(args, envs)
	return cfg, err
}

func (c *Config) LoadFrom(args []string, envs env.Provider) error {
	return cliconfig.LoadTo(c, args, envs, EnvPrefix)
}

func (c *Config) Normalize() {
	if c.ListenAddress != nil {
		c.ListenAddress.Host = strhelper.NormalizeHost(c.ListenAddress.Host)
		if c.ListenAddress.Scheme == "" {
			c.ListenAddress.Scheme = "http"
		}
	}
}

func (c *Config) Validate() error {
	errs := errors.NewMultiError()
	if c.StorageAPIHost == "" {
		errs.Append(errors.New(`StorageAPIHost must be set`))
	}
	if c.ListenAddress == nil || c.ListenAddress.String() == "" {
		errs.Append(errors.New("listen address is not set"))
	}
	if len(c.Repositories) == 0 {
		errs.Append(errors.New(`at least one default repository must be set`))
	}
	return errs.ErrorOrNil()
}

func (c Config) Apply(ops ...Option) Config {
	for _, o := range ops {
		o(&c)
	}
	return c
}

func WithListenAddress(v *url.URL) Option {
	return func(c *Config) {
		c.ListenAddress = v
	}
}
