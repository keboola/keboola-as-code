package source

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/httpsource"

type Config struct {
	HTTP httpsource.Config `configKey:"http"`
}

type ConfigPatch struct{}

func NewConfig() Config {
	return Config{
		HTTP: httpsource.NewConfig(),
	}
}
