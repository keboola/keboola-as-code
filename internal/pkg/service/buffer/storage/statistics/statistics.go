// Package statistics is responsible for collecting and providing statistics regarding the count and size of processed records.
// These statistics are primarily used to evaluate upload and import conditions,
// but users can also access them through the API.
//
// # Data Model
//
// Statistics are stored in the etcd database as [statistics.Value] under the following key format:
//
//	stats/<CATEGORY:buffered>/<SLICE_KEY:PROJECT_ID/RECEIVER_ID/EXPORT_ID/FILE_ID/SLICE_ID>/<API_NODE_ID>
//
// Statistics are stored at the slice level, which represents the smallest unit.
// To obtain statistics for a parent object such as a file, export, receiver, or project,
// simply sum up all the values under the corresponding prefix.
//
//	For a receiver:  stats/<CATEGORY>/<PROJECT_ID>/<RECEIVER_ID>/
//	For an export:   stats/<CATEGORY>/<PROJECT_ID>/<RECEIVER_ID>/<EXPORT_ID>/
//	For a file:      stats/<CATEGORY>/<PROJECT_ID>/<RECEIVER_ID>/<EXPORT_ID>/<FILE_ID>/
//	For a slice:     stats/<CATEGORY>/<PROJECT_ID>/<RECEIVER_ID>/<EXPORT_ID>/<FILE_ID>/<SLICE_ID>
//
// After a successful slice upload, the statistics category changes from "buffered" to "uploaded."
// Within it, the statistics from all nodes are aggregated and saved as:
//
//	stats/<CATEGORY:uploaded>/<SLICE_KEY:PROJECT_ID/RECEIVER_ID/EXPORT_ID/FILE_ID/SLICE_ID>/_nodes_sum
//
// After a successful file import, the statistics category changes from "uploaded" to "imported."
// The key format remains the same.
//
//	stats/<CATEGORY:imported>/<SLICE_KEY:PROJECT_ID/RECEIVER_ID/EXPORT_ID/FILE_ID/SLICE_ID>/_nodes_sum
//
// Over time, files and slices expire and are deleted during the Cleanup operation.
// Their statistics in the "buffered" and "uploaded" categories are also deleted.
// Statistics in the "imported" category are summarized under the key:
//
//	stats/<CATEGORY:imported>/<EXPORT_KEY:PROJECT_ID/RECEIVER_ID/EXPORT_ID>/_cleanup_sum
//
// # Collector
//
// Whenever a new record is successfully saved, the [Collector.Notify] method is called.
//
// Initially, statistics are stored in the API node memory within the [Collector].
//
// Regular synchronization to etcd occurs at the interval specified in the [config.APIConfig] in the StatisticsSyncInterval field.
//
// At this stage, statistics are stored per slice and API node, ensuring that import operations are not slowed down by using a lock.
//
// # Atomicity
//
// In the event of an unexpected termination of the API node running the [statistics.Collector],
// it is possible that the latest state of the statistics stored in memory did not have enough time to synchronize to etcd.
//
// This is a trade-off made to optimize the system for maximum throughput.
//
// For evaluating upload and import conditions, this is not an issue,
// as additional records will arrive or more time will pass, finally triggering the operation anyway.
//
// To address this rare potential problem, the statistics are updated via [Value.WithAfterUpload] method after the slice upload,
// ensuring that the real number of uploaded records is set to statistics.
// So the statistics can be inaccurate only during the phase when slices are buffered in etcd,
// after the upload the value will be autocorrected.
//
// # Repository
//
// The [Repository] embeds the [AtomicProvider] and includes additional methods
// for inserting statistics and making modifications from other parts of the application.
//
// # Providers
//
// The [statistics.Provider] interface defines methods to get statistics for: receiver, export, file and slice.
// There are multiple implementations of the interface, see the following section.
//
// # Atomic Provider
//
// The [statistics.AtomicProvider] loads statistics using an etcd transaction and then summarizes all the values.
// This method always provides up-to-date results but is the most time-consuming and computationally expensive.
//
// This method is used when an exact actual value is required.
//
// # L1 Cache Provider
//
// The [statistics.L1CacheProvider] keeps a copy of all statistics from the "stats/" prefix in memory.
// It is updated using the etcd Watch API by the [etcdop.Mirror] utility.
//
// Compared to the [statistics.AtomicProvider], this method calculates statistics
// by iterating through the in-memory copy, without querying etcd.
//
// Each [statistics.Value] + etcd key occupies approximately 100B in memory, meaning that 10,000 records will occupy 1MB.
//
// The values are typically a few milliseconds out of date, and some CPU power is required for the calculation.
//
// This method is primarily used for evaluating upload and import conditions, which are repeated every few seconds.
//
// # L2 Cache Provider
//
// The [statistics.L2CacheProvider] caches the calculation result from the [statistics.L1CacheProvider]
// for a specific object (such as export or file).
// Therefore, obtaining statistics does not require any further calculations.
//
// The cache is periodically invalidated, and the invalidation interval is configured in the [config.ServiceConfig]
// in the StatisticsL2CacheTTL field.
//
// This method is the least computationally expensive, but the results are also the most delayed.
// The maximum delay is the sum of the invalidation interval and a few milliseconds delay from the L1 cache.
//
// This method is primarily used in the [quota.Checker]
// to check the limit of the buffered data size at each import operation.
package statistics

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

// Value contains statistics for a slice or summarized statistics for a parent object.
type Value struct {
	// FirstRecordAt contains the timestamp of the first received record.
	FirstRecordAt utctime.UTCTime `json:"firstRecordAt" validate:"required"`
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

type PerSlice struct {
	SliceKey storage.SliceKey
	Value    Value
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

func (v Value) Add(v2 Value) Value {
	v.RecordsCount += v2.RecordsCount
	v.RecordsSize += v2.RecordsSize
	v.BodySize += v2.BodySize
	v.FileSize += v2.FileSize
	v.FileGZipSize += v2.FileGZipSize
	if v.FirstRecordAt.IsZero() || !v2.FirstRecordAt.After(v.FirstRecordAt) {
		v.FirstRecordAt = v2.FirstRecordAt
	}
	if v2.LastRecordAt.After(v.LastRecordAt) {
		v.LastRecordAt = v2.LastRecordAt
	}
	return v
}
