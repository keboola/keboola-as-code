// Package repository provides schema and database operations for storage statistics.
//
// # Data Model
//
// Statistics are stored in the etcd database as [statistics.Value] under the following key format:
//
//	storage/stats/<LEVEL:local>/<SLICE_KEY:PROJECT_ID/RECEIVER_ID/EXPORT_ID/FILE_ID/VOLUME_ID/SLICE_ID>/value
//
// Statistics are stored at the slice level, which represents the smallest unit.
//
// To obtain statistics for a parent object such as a file, export, receiver, or project,
// simply sum up all the values under the corresponding prefix.
//
//	For a project:   storage/stats/<LEVEL>/<PROJECT_ID>/<RECEIVER_ID>/
//	For a receiver:  storage/stats/<LEVEL>/<PROJECT_ID>/<RECEIVER_ID>/
//	For an export:   storage/stats/<LEVEL>/<PROJECT_ID>/<RECEIVER_ID>/<EXPORT_ID>/
//	For a file:      storage/stats/<LEVEL>/<PROJECT_ID>/<RECEIVER_ID>/<EXPORT_ID>/<FILE_ID>/
//	For a slice:     storage/stats/<LEVEL>/<PROJECT_ID>/<RECEIVER_ID>/<EXPORT_ID>/<FILE_ID>/<VOLUME_ID>/<SLICE_ID>
//
// # Rollup
//
// Over time, files and slices expire and are deleted during the Cleanup operation.
// Their statistics in the "local" and "staging" levels are also deleted.
// Statistics in the "target" level are summarized under the key:
//
//	stats/<LEVEL:target>/<EXPORT_KEY:PROJECT_ID/RECEIVER_ID/EXPORT_ID>/_sum
package repository

import (
	"context"
	"fmt"

	"github.com/c2h5oh/datasize"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	// putMaxStatsPerTxn defines maximum number of keys per transaction when updating database values.
	putMaxStatsPerTxn = 100
	// recordsForSliceDiskSizeCalc defines the number of last slice statistics that are taken into account
	// when calculating the amount of disk space that needs to be pre-allocated for a new slice.
	recordsForSliceDiskSizeCalc = 10
)

type _provider = Provider

// Repository provides database operations for storage statistics records.
type Repository struct {
	_provider
	telemetry telemetry.Telemetry
	client    *etcd.Client
	schema    schema
	plugins   *plugin.Plugins
}

type dependencies interface {
	Telemetry() telemetry.Telemetry
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func New(d dependencies) *Repository {
	r := &Repository{
		telemetry: d.Telemetry(),
		client:    d.EtcdClient(),
		schema:    newSchema(d.EtcdSerde()),
		plugins:   d.Plugins(),
	}

	// Setup Provider interface
	r._provider = NewProvider(r.aggregate)

	// Connect to file/slice events
	r.plugins.Collection().OnFileSave(func(ctx *plugin.Operation, old, updated *model.File) {
		// On file deletion: delete/rollup statistics
		if updated.Deleted {
			ctx.AddFrom(r.Delete(updated.FileKey))
			return
		}

		// On file creation: nop
		if old == nil {
			return
		}

		// On file modification: move statistics to the target storage level, if needed
		fromLevel := old.State.Level()
		toLevel := updated.State.Level()
		if fromLevel != toLevel {
			ctx.AddFrom(r.MoveAll(updated.FileKey, fromLevel, toLevel, func(value *statistics.Value) {
				// There is actually no additional compression, when uploading slice to the staging storage
				if toLevel == level.Staging {
					value.StagingSize = value.CompressedSize
				}
			}))
		}
	})
	r.plugins.Collection().OnSliceSave(func(ctx *plugin.Operation, old, updated *model.Slice) {
		// On slice deletion: delete/rollup statistics
		if updated.Deleted {
			ctx.AddFrom(r.Delete(updated.SliceKey))
			return
		}

		// On slice creation: calculate pre-allocated disk space from the size of previous slices
		if old == nil {
			ctx.AddFrom(op.Atomic(nil, &op.NoResult{}).
				Read(func(ctx context.Context) op.Op {
					// Get disk allocation config
					cfg, ok := diskalloc.ConfigFromContext(ctx)
					if !ok {
						return nil
					}

					// Calculate pre-allocated size
					return r.
						MaxUsedDiskSizeBySliceIn(updated.SinkKey, recordsForSliceDiskSizeCalc).
						OnResult(func(r *op.TxnResult[datasize.ByteSize]) {
							updated.LocalStorage.AllocatedDiskSpace = cfg.ForNextSlice(r.Result())
						})
				}),
			)
			return
		}

		// On slice modification: move statistics to the target storage level, if needed
		fromLevel := old.State.Level()
		toLevel := updated.State.Level()
		if fromLevel != toLevel {
			ctx.AddFrom(r.Move(updated.SliceKey, fromLevel, toLevel, func(value *statistics.Value) {
				// There is actually no additional compression, when uploading slice to the staging storage
				if toLevel == level.Staging {
					value.StagingSize = value.CompressedSize
				}
			}))
		}
	})

	return r
}

// ObjectPrefix returns string prefix of the object statistics in the database.
// The method is used by the cache.L1 which has in-memory mirror of the database.
func (r *Repository) ObjectPrefix(level level.Level, objectKey fmt.Stringer) string {
	return r.schema.InLevel(level).InObject(objectKey).Prefix()
}
