package model

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
)

type ImportConditions struct {
	Count int               `json:"count" validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" validate:"min=100,max=50000000"`               // 100B-50MB
	Time  time.Duration     `json:"time" validate:"min=30000000000,max=86400000000000"` // 30s-24h
}

func (c ImportConditions) ShouldImport(s CurrentImportState) (bool, string) {
	if s.Count == 0 {
		return false, "no data to import"
	}

	defaults := DefaultConditions()
	if c.Count == 0 {
		c.Count = defaults.Count
	}
	if c.Size == 0 {
		c.Size = defaults.Size
	}
	if c.Time == 0 {
		c.Time = defaults.Time
	}

	if s.Count >= c.Count {
		return true, fmt.Sprintf("import count threshold met, received: %d rows, threshold: %d rows", s.Count, c.Count)
	}
	if s.Size >= c.Size {
		return true, fmt.Sprintf("import size threshold met, received: %s, threshold: %s", s.Size.String(), c.Size.String())
	}

	now := time.Time(s.Now)
	lastImportAt := time.Time(s.LastImportAt)
	sinceLastImport := now.Sub(lastImportAt).Truncate(time.Second)
	if sinceLastImport >= c.Time {
		return true, fmt.Sprintf("import time threshold met, last import at: %s, passed: %s threshold: %s", lastImportAt.Format(TimeFormat), sinceLastImport.String(), c.Time.String())
	}
	return false, "conditions not met"
}
