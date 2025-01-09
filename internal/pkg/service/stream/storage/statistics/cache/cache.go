// Package cache provides caching of aggregated storage statistics
// to reduce the number of requests to databases and the time needed to obtain results.
//
// The L1 cache contains in-memory etcdop.Mirror of all statistics in the database.
//
// The L2 cache is implemented on top of the L1 cache, it caches final aggregated value for the object.
package cache

import (
	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Process() *servicectx.Process
	StatisticsRepository() *statsRepo.Repository
}
