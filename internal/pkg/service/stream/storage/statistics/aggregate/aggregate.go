// Package aggregate provides aggregation of statistics for each storage level.
package aggregate

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Aggregate adds a partial statistics from the level to the aggregated result.
func Aggregate(l level.Level, partial statistics.Value, out *statistics.Aggregated) {
	switch l {
	case level.Local:
		out.Local = out.Local.Add(partial)
		out.Total = out.Total.Add(partial)
	case level.Staging:
		out.Staging = out.Staging.Add(partial)
		out.Total = out.Total.Add(partial)
	case level.Target:
		out.Target = out.Target.Add(partial)
		out.Total = out.Total.Add(partial)
	default:
		panic(errors.Errorf(`unexpected statistics level "%v"`, l))
	}
}
