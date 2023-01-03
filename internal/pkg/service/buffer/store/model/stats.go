package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type StatsProvider interface {
	GetStats() Stats
}

type Stats struct {
	Count  uint64      `json:"count" validate:"required"`
	Size   uint64      `json:"size" validate:"required"`
	LastAt key.UTCTime `json:"lastAt" validate:"required"`
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
			Count:  count,
			Size:   size,
			LastAt: key.UTCTime(lastReceivedAt),
		},
	}
}

func (s Stats) GetStats() Stats {
	return s
}
