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
	// LastRecordAt contains the timestamp of the last received/uploaded/imported record, according to the type of statistics.
	LastRecordAt UTCTime `json:"lastRecordAt" validate:"required"`
	// RecordsCount is count of received/uploaded/imported records.
	RecordsCount uint64 `json:"recordsCount" validate:"required"`
	// RecordsSize is total size of CSV rows.
	RecordsSize uint64 `json:"recordsSize"`
	// BodySize is total size of all processed request bodies.
	BodySize uint64 `json:"bodySize"`
	// FileSize total file size before compression.
	FileSize uint64 `json:"fileSize,omitempty"`
	// FileSize total file size after compression.
	FileGZipSize uint64 `json:"fileGZipSize,omitempty"`
}

type SliceStats struct {
	key.SliceKey
	Stats
}

func (s Stats) GetStats() Stats {
	return s
}
