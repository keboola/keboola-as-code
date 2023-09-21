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
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	// putMaxStatsPerTxn defines maximum number of keys per transaction when updating database values.
	putMaxStatsPerTxn = 100
)

type _provider = Provider

// Repository provides database operations for storage statistics records.
type Repository struct {
	_provider
	telemetry telemetry.Telemetry
	client    *etcd.Client
	schema    SchemaRoot
}

type dependencies interface {
	Telemetry() telemetry.Telemetry
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func New(d dependencies) *Repository {
	r := &Repository{
		telemetry: d.Telemetry(),
		client:    d.EtcdClient(),
		schema:    newSchema(d.EtcdSerde()),
	}

	// Setup Provider interface
	r._provider = NewProvider(r.aggregate)

	return r
}

func (r *Repository) Schema() SchemaRoot {
	return r.schema
}

func (r *Repository) MoveOp(ctx context.Context, sliceKey storage.SliceKey, from, to storage.Level, modifyStatsFn func(*Value)) (op.Op, error) {
	if from == to {
		panic(errors.Errorf(`from and to categories are same and equal to "%s"`, to))
	}

	fromKey := r.schema.InLevel(from).InSlice(sliceKey)
	toKey := r.schema.InLevel(to).InSlice(sliceKey)

	stats, err := fromKey.Get().Do(ctx, r.client)
	if err != nil {
		return nil, err
	}

	if modifyStatsFn != nil {
		modifyStatsFn(&stats.Value)
	}

	return op.MergeToTxn(fromKey.Delete(), toKey.Put(stats.Value)), nil
}
