// Package aggregate provides aggregation of statistics for each storage level.
package aggregate

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Aggregate adds a partial statistics from the level to the aggregated result.
func Aggregate(l model.Level, partial statistics.Value, out *statistics.Aggregated) {
	switch l {
	case model.LevelLocal:
		out.Local = out.Local.Add(partial)
		out.Total = out.Total.Add(partial)
	case model.LevelStaging:
		out.Staging = out.Staging.Add(partial)
		out.Total = out.Total.Add(partial)
	case model.LevelTarget:
		out.Target = out.Target.Add(partial)
		out.Total = out.Total.Add(partial)
	default:
		panic(errors.Errorf(`unexpected statistics level "%v"`, l))
	}
}
