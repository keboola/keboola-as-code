package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

// StatsProvider is common interface for all statistics.
type StatsProvider interface {
	GetStats() Stats
}

// Stats struct is common for received/uploaded/imported statistics.
type Stats struct {
	Count uint64 `json:"count" validate:"required"`
	Size  uint64 `json:"size" validate:"required"`
	// LastRecordAt contains the timestamp of the last received/uploaded/imported record, according to the type of statistics.
	LastRecordAt key.UTCTime `json:"lastRecordAt" validate:"required"`
}

type SliceStats struct {
	key.SliceKey
	Stats
}

func NewSliceStats(
	sliceKey key.SliceKey,
	count uint64,
	size uint64,
	lastReceivedAt key.ReceivedAt,
) SliceStats {
	return SliceStats{
		SliceKey: sliceKey,
		Stats: Stats{
			Count:        count,
			Size:         size,
			LastRecordAt: key.UTCTime(lastReceivedAt),
		},
	}
}

func (s Stats) GetStats() Stats {
	return s
}
