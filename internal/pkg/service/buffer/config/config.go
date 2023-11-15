package config

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	EnvPrefix = "STREAM_"
)

// Config of the Stream services.
type Config struct {
	Storage storage.Config `configKey:"storage"`
}

func New() Config {
	return Config{
		Storage: storage.NewConfig(),
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

}

func (c *Config) Validate() error {
	v := validator.New()
	return v.Validate(context.Background(), c)
}
