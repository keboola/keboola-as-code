package iterator

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

const DefaultLimit = 100

type Option func(c *config)

type config struct {
	limit int
	start string
	serde serde.Serde
}

func WithLimit(limit int) Option {
	return func(c *config) {
		c.limit = limit
	}
}
