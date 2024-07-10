package sink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink"
)

type Config struct {
	Table tablesink.Config `configKey:"table"`
}

type ConfigPatch struct {
	Table *tablesink.ConfigPatch `json:"table,omitempty"`
}

func NewConfig() Config {
	return Config{
		Table: tablesink.NewConfig(),
	}
}
