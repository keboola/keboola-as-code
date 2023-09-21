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
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
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
	schema    schemaRoot
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

// ObjectPrefix returns string prefix of the object statistics in the database.
// The method is used by the cache.L1 which has in-memory mirror of the database.
func (r *Repository) ObjectPrefix(level storage.Level, objectKey fmt.Stringer) string {
	return r.schema.InLevel(level).InObject(objectKey).Prefix()
}
