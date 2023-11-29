package sink

import "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink"

type Config struct {
	Table tablesink.Config `configKey:"table"`
}

func NewConfig() Config {
	return Config{
		Table: tablesink.NewConfig(),
	}
}
