// Package statistics provides:
// - Collecting of statistics from the API node import endpoint.
// - Caching of statistics used by of the upload and import conditions resolver.

package statistics

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics/collector"
)

type (
	CollectorNode = collector.Node
	CacheNode     = cache.Node
)

func NewCollectorNode(d collector.Dependencies) *CollectorNode {
	return collector.NewNode(d)
}

func NewCacheNode(d cache.Dependencies) (*CacheNode, error) {
	return cache.NewNode(d)
}
