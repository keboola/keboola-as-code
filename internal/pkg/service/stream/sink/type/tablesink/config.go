package tablesink

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"

type Config struct {
	Keboola keboola.Config `configKey:"keboola"`
}

type ConfigPatch struct{}

func NewConfig() Config {
	return Config{
		Keboola: keboola.NewConfig(),
	}
}
