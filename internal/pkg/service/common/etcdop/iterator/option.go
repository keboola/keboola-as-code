package iterator

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const DefaultLimit = 100

type Option func(c *config)

type config struct {
	prefix       string
	end          string       // optional range end, it is a suffix to the prefix field
	serde        *serde.Serde // empty for not-typed iterator
	pageSize     int
	revision     int64 // revision of the all values, set by "WithRev" or by the first page
	fromSameRev  bool  // fromSameRev if true, then 2+ page will be loaded from the same revision as the first page
	serializable bool  // serializable requests are better for lower latency. see etcd documentation
}

func newConfig(prefix string, s *serde.Serde, opts []Option) config {
	c := config{
		prefix:       prefix,
		end:          etcd.GetPrefixRangeEnd(prefix), // default range end, read the entire prefix
		serde:        s,
		pageSize:     DefaultLimit,
		fromSameRev:  true,
		serializable: false,
	}

	// Apply options
	for _, o := range opts {
		o(&c)
	}

	return c
}

func WithPageSize(v int) Option {
	if v < 1 {
		panic(errors.New("page size must be greater than 0"))
	}
	return func(c *config) {
		c.pageSize = v
	}
}

func WithRev(v int64) Option {
	if v <= 0 {
		panic(errors.New("revision must be greater than 0"))
	}
	return func(c *config) {
		c.revision = v
		if v > 0 {
			c.fromSameRev = false
		}
	}
}

// WithFromSameRev - if true, then 2+ page will be loaded from the same revision as the first page.
// It is incompatible with specifying exact revision via WithRev.
func WithFromSameRev(v bool) Option {
	return func(c *config) {
		c.fromSameRev = v
		if v {
			c.revision = 0
		}
	}
}

// WithSerializable makes 'Get' request serializable. By default, it's linearizable.
// Serializable requests are better for lower latency.
func WithSerializable() Option {
	return func(c *config) {
		c.serializable = true
	}
}

// WithEnd defines end of the iteration, all keys from the range [prefix/, prefix/end) will be loaded.
func WithEnd(v string) Option {
	return func(c *config) {
		c.end = c.prefix + v
	}
}
