package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type SliceStats struct {
	key.SliceStatsKey
	Count          uint64    `json:"count" validate:"required"`
	Size           uint64    `json:"size" validate:"required"`
	LastReceivedAt time.Time `json:"lastReceivedAt" validate:"required"`
}

func NewSliceStats(
	key key.SliceStatsKey,
	count uint64,
	size uint64,
	lastReceivedAt time.Time,
) SliceStats {
	return SliceStats{
		SliceStatsKey:  key,
		Count:          count,
		Size:           size,
		LastReceivedAt: lastReceivedAt,
	}
}
