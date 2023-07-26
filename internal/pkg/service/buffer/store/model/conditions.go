package model

import (
	"time"

	"github.com/c2h5oh/datasize"
)

// Conditions struct configures slice upload and file import conditions.
type Conditions struct {
	Count uint64            `json:"count" mapstructure:"count" usage:"Records count." validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" mapstructure:"size" usage:"Records size." validate:"minBytes=100B,maxBytes=50MB"`
	Time  time.Duration     `json:"time" mapstructure:"time" usage:"Duration from the last upload/import." validate:"minDuration=30s,maxDuration=24h"`
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
