package model

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type Conditions config.Conditions

// DefaultImportConditions determines when a file will be imported to a table.
// These settings are configurable per export, see Export.ImportConditions.
func DefaultImportConditions() Conditions {
	return Conditions{
		Count: 10000,
		Size:  5 * datasize.MB,
		Time:  5 * time.Minute,
	}
}

func (c Conditions) Evaluate(now time.Time, openedAt time.Time, s statistics.Value) (bool, string) {
	if s.RecordsCount == 0 {
		return false, "no record"
	}

	if s.RecordsCount >= c.Count {
		return true, fmt.Sprintf("count threshold met, received: %d rows, threshold: %d rows", s.RecordsCount, c.Count)
	}
	if s.RecordsSize >= c.Size {
		return true, fmt.Sprintf("size threshold met, received: %s, threshold: %s", s.RecordsSize.HumanReadable(), c.Size.HumanReadable())
	}

	sinceOpened := now.Sub(openedAt).Truncate(time.Second)
	if sinceOpened >= c.Time {
		return true, fmt.Sprintf("time threshold met, opened at: %s, passed: %s threshold: %s", openedAt.Format(utctime.TimeFormat), sinceOpened.String(), c.Time.String())
	}

	return false, "no condition met"
}
