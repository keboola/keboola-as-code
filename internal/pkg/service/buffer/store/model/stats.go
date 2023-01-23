package model

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

// StatsProvider is common interface for all statistics.
type StatsProvider interface {
	GetStats() Stats
}

// Stats struct is common for received/uploaded/imported statistics.
type Stats struct {
	// LastRecordAt contains the timestamp of the last received record.
	LastRecordAt UTCTime `json:"lastRecordAt" validate:"required"`
	// RecordsCount is count of received records.
	RecordsCount uint64 `json:"recordsCount" validate:"required"`
	// RecordsSize is total size of CSV rows.
	RecordsSize datasize.ByteSize `json:"recordsSize"`
	// BodySize is total size of all processed request bodies.
	BodySize datasize.ByteSize `json:"bodySize"`
	// FileSize is total uploaded size before compression.
	FileSize datasize.ByteSize `json:"fileSize,omitempty"`
	// FileSize is total uploaded size after compression.
	FileGZipSize datasize.ByteSize `json:"fileGZipSize,omitempty"`
}

type SliceStats struct {
	key.SliceKey
	Stats
}

func (s Stats) GetStats() Stats {
	return s
}
