package config

import (
	"net/http"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	serviceConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	EnvPrefix = "BUFFER_WORKER_"
	// DefaultCheckConditionsInterval defines how often it will be checked upload and import conditions.
	DefaultCheckConditionsInterval = 30 * time.Second
	// DefaultCleanupInterval defines how often old tasks and files will be checked and deleted.
	DefaultCleanupInterval = 1 * time.Hour
)

// Config of the Buffer Worker.
type Config struct {
	ServiceConfig           `mapstructure:",squash"`
	CheckConditionsInterval time.Duration    `mapstructure:"check-conditions-interval" usage:"How often will upload and import conditions be checked."`
	CleanupInterval         time.Duration    `mapstructure:"cleanup-interval" usage:"How often will old resources be deleted."`
	UploadConditions        model.Conditions `mapstructure:"upload-conditions"`
	ConditionsCheck         bool             `mapstructure:"enable-conditions-check" usage:"Enable conditions check functionality."`
	CloseSlices             bool             `mapstructure:"enable-close-slices" usage:"Enable close slices functionality."`
	UploadSlices            bool             `mapstructure:"enable-upload-slices" usage:"Enable upload slices functionality."`
	RetryFailedSlices       bool             `mapstructure:"enable-retry-failed-slices" usage:"Enable retry for failed slices."`
	CloseFiles              bool             `mapstructure:"enable-close-files" usage:"Enable close files functionality."`
	ImportFiles             bool             `mapstructure:"enable-import-files" usage:"Enable import files functionality."`
	RetryFailedFiles        bool             `mapstructure:"enable-retry-failed-files" usage:"Enable retry for failed files."`
	Cleanup                 bool             `mapstructure:"enable-cleanup" usage:"Enable cleanup functionality."`
	UploadTransport         http.RoundTripper
}

type ServiceConfig = serviceConfig.Config

func NewConfig() Config {
	return Config{
		ServiceConfig:           serviceConfig.NewConfig(),
		CheckConditionsInterval: DefaultCheckConditionsInterval,
		CleanupInterval:         DefaultCleanupInterval,
		UploadConditions:        model.DefaultUploadConditions(),
		ConditionsCheck:         true,
		CloseSlices:             true,
		UploadSlices:            true,
		RetryFailedSlices:       true,
		CloseFiles:              true,
		ImportFiles:             true,
		RetryFailedFiles:        true,
		Cleanup:                 true,
		UploadTransport:         nil, // use default transport
	}
}

type Option func(c *Config)

func LoadFrom(args []string, envs env.Provider) (Config, error) {
	cfg := NewConfig()
	err := cfg.LoadFrom(args, envs)
	return cfg, err
}

func (c *Config) LoadFrom(args []string, envs env.Provider) error {
	return cliconfig.LoadTo(c, args, envs, EnvPrefix)
}

func (c *Config) Normalize() {
	c.ServiceConfig.Normalize()
}

func (c *Config) Validate() error {
	if err := c.ServiceConfig.Validate(); err != nil {
		return err
	}
	if c.CheckConditionsInterval <= 0 {
		return errors.Errorf(`CheckConditionsInterval must be positive time.Duration, found "%v"`, c.CheckConditionsInterval)
	}
	if c.CleanupInterval <= 0 {
		return errors.Errorf(`CleanupInterval must be positive time.Duration, found "%v"`, c.CleanupInterval)
	}
	if c.UploadConditions.Count <= 0 {
		return errors.Errorf(`UploadConditions.Count must be positive number, found "%v"`, c.UploadConditions.Count)
	}
	if c.UploadConditions.Time <= 0 {
		return errors.Errorf(`UploadConditions.Time must be positive time.Duration, found "%v"`, c.UploadConditions.Time.String())
	}
	if c.UploadConditions.Size <= 0 {
		return errors.Errorf(`UploadConditions.Size must be positive number, found "%v"`, c.UploadConditions.Size.String())
	}
	return nil
}

func (c Config) Apply(ops ...Option) Config {
	for _, o := range ops {
		o(&c)
	}
	return c
}

func WithCleanupInterval(v time.Duration) Option {
	return func(c *Config) {
		c.CleanupInterval = v
	}
}

func WithCheckConditionsInterval(v time.Duration) Option {
	return func(c *Config) {
		c.CheckConditionsInterval = v
	}
}

func WithUploadConditions(v model.Conditions) Option {
	return func(c *Config) {
		c.UploadConditions = v
	}
}

// WithConditionsCheck enables/disables the conditions checker.
func WithConditionsCheck(v bool) Option {
	return func(c *Config) {
		c.ConditionsCheck = v
	}
}

// WithCleanup enables/disables etcd cleanup task.
func WithCleanup(v bool) Option {
	return func(c *Config) {
		c.Cleanup = v
	}
}

// WithCloseSlices enables/disables the "close slices" task.
func WithCloseSlices(v bool) Option {
	return func(c *Config) {
		c.CloseSlices = v
	}
}

// WithUploadSlices enables/disables the "upload slices" task.
func WithUploadSlices(v bool) Option {
	return func(c *Config) {
		c.UploadSlices = v
	}
}

// WithRetryFailedSlices enables/disables the "retry failed uploads" task.
func WithRetryFailedSlices(v bool) Option {
	return func(c *Config) {
		c.RetryFailedSlices = v
	}
}

// WithUploadTransport overwrites default HTTP transport.
func WithUploadTransport(v http.RoundTripper) Option {
	return func(c *Config) {
		c.UploadTransport = v
	}
}

// WithCloseFiles enables/disables the "close files" task.
func WithCloseFiles(v bool) Option {
	return func(c *Config) {
		c.CloseFiles = v
	}
}

// WithImportFiles enables/disables the "upload file" task.
func WithImportFiles(v bool) Option {
	return func(c *Config) {
		c.ImportFiles = v
	}
}

// WithRetryFailedFiles enables/disables the "retry failed imports" task.
func WithRetryFailedFiles(v bool) Option {
	return func(c *Config) {
		c.RetryFailedFiles = v
	}
}
