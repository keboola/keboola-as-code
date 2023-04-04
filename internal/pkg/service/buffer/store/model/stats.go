package model

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	commonModel "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/model"
)

// StatsProvider is common interface for all statistics.
type StatsProvider interface {
	GetStats() Stats
}

// Stats struct is common for received/uploaded/imported statistics.
type Stats struct {
	// LastRecordAt contains the timestamp of the last received record.
	LastRecordAt commonModel.UTCTime `json:"lastRecordAt" validate:"required"`
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

type StatsByType struct {
	// Received = active + closed + uploaded
	Total Stats
	// Buffered = all in active, closed state group, size of buffered data in the etcd
	Buffered Stats
	// Uploading = all in closed state group, in the process of uploading from the etcd to the file storage
	Uploading Stats
	// Uploaded = all in uploaded state group, uploaded in the file storage
	Uploaded Stats
}

type SliceStats struct {
	key.SliceNodeKey
	Stats
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
