package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type SliceStats struct {
	key.SliceKey
	Count          uint64    `json:"count" validate:"required"`
	Size           uint64    `json:"size" validate:"required"`
	LastReceivedAt time.Time `json:"lastReceivedAt" validate:"required"`
}

func NewSliceStats(
	key key.SliceKey,
	count uint64,
	size uint64,
	lastReceivedAt time.Time,
) SliceStats {
	return SliceStats{
		SliceKey:       key,
		Count:          count,
		Size:           size,
		LastReceivedAt: lastReceivedAt,
	}
}
