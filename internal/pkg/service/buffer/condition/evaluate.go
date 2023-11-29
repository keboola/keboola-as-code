package condition

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func evaluate(def Conditions, now time.Time, openedAt time.Time, stats statistics.Value) (bool, string) {
	if stats.RecordsCount == 0 {
		return false, "no record"
	}

	if stats.RecordsCount >= def.Count {
		return true, fmt.Sprintf("count threshold met, received: %d rows, threshold: %d rows", stats.RecordsCount, def.Count)
	}
	if stats.RecordsSize >= def.Size {
		return true, fmt.Sprintf("size threshold met, received: %s, threshold: %s", stats.RecordsSize.HumanReadable(), def.Size.HumanReadable())
	}

	sinceOpened := now.Sub(openedAt).Truncate(time.Second)
	if sinceOpened >= def.Time {
		return true, fmt.Sprintf("time threshold met, opened at: %s, passed: %s threshold: %s", openedAt.Format(utctime.TimeFormat), sinceOpened.String(), def.Time.String())
	}

	return false, "no condition met"
}
