// Package aggregate provides aggregation of statistics for each storage level.
package aggregate

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
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

func AggregateIntervalGroup(l level.Level, partial statistics.Value, since utctime.UTCTime, duration time.Duration, out *statistics.IntervalGroup) {
	i := partial.FirstRecordAt.Time().Sub(since.Time()) / duration
	Aggregate(l, partial, &out.Intervals[i].Levels)
	Aggregate(l, partial, &out.Total.Levels)
}
