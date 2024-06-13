// Package statistics is responsible for collecting and providing statistics regarding the count and size of processed records.
//
// These statistics are primarily used to evaluate upload and import conditions,
// but users can also access them through the API.
//
// Statistics are collected and stored per definition.Sink, one definition.Source can have multiple sinks,
// so one message, received by a source, is counted several times, according to the number of sinks.
//
// The statistics Collector collects statistics from each active slice writer on each source node.
//
// Statistics are saved per the slice and the source node, see OpenSlice and Put method in the repository.
// Later, the rollup operation is performed, see deleteOrRollup method.
package statistics

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// Value contains statistics for a slice or summarized statistics for a parent object.
type Value struct {
	SlicesCount uint64 `json:"slicesCount,omitempty"`
	// FirstRecordAt contains the timestamp of the first received record.
	FirstRecordAt utctime.UTCTime `json:"firstRecordAt"`
	// LastRecordAt contains the timestamp of the last received record.
	LastRecordAt utctime.UTCTime `json:"lastRecordAt"`
	// RecordsCount is count of received records.
	RecordsCount uint64 `json:"recordsCount,omitempty"`
	// UncompressedSize is data size before compression in the local storage.
	UncompressedSize datasize.ByteSize `json:"uncompressedSize,omitempty"`
	// CompressedSize is data size after compression in the local storage.
	CompressedSize datasize.ByteSize `json:"compressedSize,omitempty"`
	// StagingSize is data size in the staging storage.
	// The value is usually same as the CompressedSize,
	// if the type of compression did not change during the upload.
	StagingSize datasize.ByteSize `json:"stagingSize,omitempty"`
	// Reset is true if the value is considered negative.
	Reset bool `json:"reset,omitempty"`
}

type PerSlice struct {
	SliceKey model.SliceKey

	// The following fields are part of the Value structure, which are applicable when collecting statistics.

	FirstRecordAt    utctime.UTCTime
	LastRecordAt     utctime.UTCTime
	RecordsCount     uint64
	UncompressedSize datasize.ByteSize
	CompressedSize   datasize.ByteSize
}

// Aggregated contains aggregated statistics for an object, such as file or export.
type Aggregated struct {
	// Local field contains summarized statistics for slices in storage.SliceWriting, storage.SliceClosing, storage.SliceUploading.
	// Statistics match the data on local disks.
	Local Value
	// Staging field contains summarized statistics for slices in storage.SliceUploaded.
	// Statistics match the data in the staging storage.
	Staging Value
	// Target  field contains summarized statistics for slices in storage.SliceImported.
	// Statistics match the data in the target table.
	Target Value
	// Total field contains summarized statistics for slices in all states, Local + Staging + Target.
	Total Value
}

// Add returns a sum of the value and the given second value.
// If only one of the values is a reset value then it returns a non-reset value using subtraction.
// Note that the value can't be negative, in case of an underflow the fields will be 0.
func (v Value) Add(v2 Value) Value {
	if v.Reset != v2.Reset {
		if v.Reset {
			return v2.Add(v)
		}
		if v2.SlicesCount > v.SlicesCount {
			v.SlicesCount = 0
		} else {
			v.SlicesCount -= v2.SlicesCount
		}
		if v2.RecordsCount > v.RecordsCount {
			v.RecordsCount = 0
		} else {
			v.RecordsCount -= v2.RecordsCount
		}
		if v2.UncompressedSize > v.UncompressedSize {
			v.UncompressedSize = 0
		} else {
			v.UncompressedSize -= v2.UncompressedSize
		}
		if v2.CompressedSize > v.CompressedSize {
			v.CompressedSize = 0
		} else {
			v.CompressedSize -= v2.CompressedSize
		}
		if v2.StagingSize > v.StagingSize {
			v.StagingSize = 0
		} else {
			v.StagingSize -= v2.StagingSize
		}
	} else {
		v.SlicesCount += v2.SlicesCount
		v.RecordsCount += v2.RecordsCount
		v.UncompressedSize += v2.UncompressedSize
		v.CompressedSize += v2.CompressedSize
		v.StagingSize += v2.StagingSize
	}
	if v.FirstRecordAt.IsZero() || (!v2.FirstRecordAt.IsZero() && v.FirstRecordAt.After(v2.FirstRecordAt)) {
		v.FirstRecordAt = v2.FirstRecordAt
	}
	if v2.LastRecordAt.After(v.LastRecordAt) {
		v.LastRecordAt = v2.LastRecordAt
	}
	return v
}
