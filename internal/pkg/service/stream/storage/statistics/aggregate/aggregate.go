// Package aggregate provides aggregation of statistics for each storage level.
package aggregate

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// Aggregate adds a partial statistics from the level to the aggregated result.
func Aggregate(l model.Level, partial statistics.Value, out *statistics.Aggregated) {
	out.Add(l, partial)
}
