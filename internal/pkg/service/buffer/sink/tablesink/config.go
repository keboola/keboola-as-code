package tablesink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
)

type Config struct {
	Statistics statistics.Config `configKey:"statistics"`
	Storage    storage.Config    `configKey:"storage"`
}

func NewConfig() Config {
	return Config{
		Statistics: statistics.NewConfig(),
		Storage:    storage.NewConfig(),
	}
}
