package iterator

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const DefaultLimit = 100

type Option func(c *config)

type config struct {
	client etcd.KV
	prefix string
	// startOffset, relative to the prefix, the specified key is excluded
	startOffset         string
	startOffsetIncluded bool
	// endOffset, relative to the prefix, the specified key is excluded
	endOffset         string
	endOffsetIncluded bool
	// sort - etcd.SortAscend  etcd.Sort.Descend
	sort etcd.SortOrder
	// limit is maximum number of iterated records
	limit int
	// records per one page, per one GET operation
	pageSize int
	// revision of the all values, set by "WithRev" or by the first page
	revision int64
	// fromSameRev if true, then 2+ page will be loaded from the same revision as the first page
	fromSameRev bool
}

func newConfig(client etcd.KV, prefix string, opts []Option) config {
	c := config{
		prefix:      prefix,
		sort:        etcd.SortAscend,
		client:      client,
		pageSize:    DefaultLimit,
		fromSameRev: true,
	}

	// Apply options
	for _, o := range opts {
		o(&c)
	}

	return c
}

func WithSort(v etcd.SortOrder) Option {
	if v != etcd.SortAscend && v != etcd.SortDescend {
		panic(errors.New("sort must be SortAscend or SortDescend"))
	}
	return func(c *config) {
		c.sort = v
	}
}

func WithLimit(v int) Option {
	if v < 1 {
		panic(errors.New("limit must be greater than 0"))
	}
	return func(c *config) {
		c.limit = v
	}
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

// WithStartOffset defines start of the iteration relative to the prefix.
// An empty string (default) means that the start is the first key in the prefix.
// Iterated are all keys from the range (prefix/startOffset, prefix/endOffset).
// If the included == false, the iteration starts with the key, that is lexicographically after the specified value.
func WithStartOffset(v string, included bool) Option {
	return func(c *config) {
		c.startOffset = v
		c.startOffsetIncluded = included
	}
}

// WithEndOffset defines end of the iteration relative to the prefix.
// An empty string (default) means that the end is the last key in the prefix.
// Iterated are all keys from the range (prefix/startOffset, prefix/endOffset).
// If the included == false, the iteration ends with the key, that is lexicographically before the specified value.
func WithEndOffset(v string, included bool) Option {
	return func(c *config) {
		c.endOffset = v
		c.endOffsetIncluded = included
	}
}

func (c config) start() string {
	if c.startOffset != "" {
		if c.startOffsetIncluded {
			// Iterate from the startOffset, the startOffset is included.
			return c.prefix + c.startOffset
		} else {
			// Iterate from the startOffset, the startOffset is excluded.
			return etcd.GetPrefixRangeEnd(c.prefix + c.startOffset)
		}
	}
	// Iterate from the first key in the prefix.
	return c.prefix
}

func (c config) end() string {
	if c.endOffset != "" {
		if c.endOffsetIncluded {
			// Iterate to the endOffset, the endOffset is included.
			return etcd.GetPrefixRangeEnd(c.prefix + c.endOffset)
		} else {
			// Iterate to the endOffset, the endOffset is excluded.
			return c.prefix + c.endOffset
		}
	}
	// Iterate to the last key in the prefix.
	return etcd.GetPrefixRangeEnd(c.prefix)
}
