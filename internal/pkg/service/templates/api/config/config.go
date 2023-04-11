package config

import (
	"net/url"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	EnvPrefix      = "TEMPLATES_API_"
	SpanNamePrefix = "keboola.go.templates.task"
)

// Config of the Templates API.
// See "cliconfig" package for more information.
type Config struct {
	Debug              bool          `mapstructure:"debug" usage:"Enable debug log level."`
	DebugHTTP          bool          `mapstructure:"debug-http" usage:"Log HTTP client request and response bodies."`
	DatadogEnabled     bool          `mapstructure:"datadog-enabled" usage:"Enable Datadog telemetry integration."`
	DatadogDebug       bool          `mapstructure:"datadog-debug" usage:"Enable Datadog debug logs."`
	CpuProfFilePath    string        `mapstructure:"cpu-profile" usage:"Write cpu profile to the file."`
	ListenAddress      *url.URL      `mapstructure:"listen-address" usage:"Server listen address."`
	StorageAPIHost     string        `mapstructure:"storage-api-host" usage:"Host of the Storage API."`
	EtcdConnectTimeout time.Duration `mapstructure:"etcd-connect-timeout" usage:"etcd connect timeout."`
	EtcdEndpoint       string        `mapstructure:"etcd-endpoint" usage:"etcd endpoint."`
	EtcdNamespace      string        `mapstructure:"etcd-namespace" usage:"etcd namespace."`
	EtcdUsername       string        `mapstructure:"etcd-username" usage:"etcd username."`
	EtcdPassword       string        `mapstructure:"etcd-password" usage:"etcd password."`
	PublicAddress      *url.URL      `mapstructure:"public-address" usage:"Public address of the Templates API, to generate a link URL."`
	Repositories       Repositories  `mapstructure:"repositories" usage:"Default repositories, <name1>|<repo1>|<branch1>;..."`
}

type Repositories []model.TemplateRepository

type Option func(c *Config)

func NewConfig() Config {
	return Config{
		Debug:              false,
		DebugHTTP:          false,
		CpuProfFilePath:    "",
		DatadogEnabled:     true,
		DatadogDebug:       false,
		StorageAPIHost:     "",
		EtcdEndpoint:       "",
		EtcdNamespace:      "",
		EtcdUsername:       "",
		EtcdPassword:       "",
		EtcdConnectTimeout: 30 * time.Second, // longer default timeout, the etcd could be started at the same time as the API
		ListenAddress:      &url.URL{Scheme: "http", Host: "0.0.0.0:8000"},
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
	if c.PublicAddress != nil {
		c.PublicAddress.Host = strhelper.NormalizeHost(c.PublicAddress.Host)
		if c.PublicAddress.Scheme == "" {
			c.PublicAddress.Scheme = "https"
		}
	}
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
	if c.PublicAddress == nil || c.PublicAddress.String() == "" {
		errs.Append(errors.New("public address is not set"))
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

func WithPublicAddress(v *url.URL) Option {
	return func(c *Config) {
		c.PublicAddress = v
	}
}

func WithListenAddress(v *url.URL) Option {
	return func(c *Config) {
		c.ListenAddress = v
	}
}
