package model

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

// StatsProvider is common interface for all statistics.
type StatsProvider interface {
	GetStats() Stats
}

// Stats struct is common for received/uploaded/imported statistics.
type Stats struct {
	// LastRecordAt contains the timestamp of the last received record.
	LastRecordAt utctime.UTCTime `json:"lastRecordAt" validate:"required"`
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

type UploadStats struct {
	// RecordsCount is count of uploaded records.
	RecordsCount uint64
	// FileSize is total uploaded size before compression.
	FileSize datasize.ByteSize
	// FileSize is total uploaded size after compression.
	FileGZipSize datasize.ByteSize
}

type SliceAPINodeStats struct {
	NodeID   string
	SliceKey key.SliceKey
	Stats    Stats
}

type StatsByType struct {
	Opened       Stats
	Uploading    Stats
	Uploaded     Stats
	UploadFailed Stats
	Imported     Stats
	// AggregatedTotal = all states
	AggregatedTotal Stats
	// AggregatedInBuffer = Writing + Closing + Uploading + Failed
	AggregatedInBuffer Stats
}

func (s Stats) GetStats() Stats {
	return s
}

func (s Stats) Add(v Stats) Stats {
	s.RecordsCount += v.RecordsCount
	s.RecordsSize += v.RecordsSize
	s.BodySize += v.BodySize
	s.FileSize += v.FileSize
	s.FileGZipSize += v.FileGZipSize
	if v.LastRecordAt.After(s.LastRecordAt) {
		s.LastRecordAt = v.LastRecordAt
	}
	return s
}
