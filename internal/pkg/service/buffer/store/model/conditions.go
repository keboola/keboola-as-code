package model

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

// Conditions struct configures slice upload and file import conditions.
type Conditions struct {
	Count uint64            `json:"count" mapstructure:"count" usage:"Records count." validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" mapstructure:"size" usage:"Records size." validate:"minBytes=100B,maxBytes=50MB"`
	Time  time.Duration     `json:"time" mapstructure:"time" usage:"Duration from the last upload/import." validate:"minDuration=30s,maxDuration=24h"`
}

// DefaultUploadConditions determines when a slice will be uploaded.
// These settings are not configurable by user.
func DefaultUploadConditions() Conditions {
	return Conditions{
		Count: 1000,
		Size:  1 * datasize.MB,
		Time:  1 * time.Minute,
	}
}

// DefaultImportConditions determines when a file will be imported to a table.
// These settings are configurable per export, see Export.ImportConditions.
func DefaultImportConditions() Conditions {
	return Conditions{
		Count: 10000,
		Size:  5 * datasize.MB,
		Time:  5 * time.Minute,
	}
}

func (c Conditions) Evaluate(now time.Time, openedAt time.Time, s Stats) (bool, string) {
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
