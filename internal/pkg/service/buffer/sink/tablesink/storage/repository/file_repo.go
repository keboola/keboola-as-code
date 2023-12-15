package repository

import (
	"context"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	etcd "go.etcd.io/etcd/client/v3"
	"path/filepath"
	"time"
)

type FileRepository struct {
	clock   clock.Clock
	client  etcd.KV
	schema  fileSchema
	config  storage.Config
	backoff storage.RetryBackoff
	all     *Repository
}

func newFileRepository(d dependencies, config storage.Config, backoff storage.RetryBackoff, all *Repository) *FileRepository {
	return &FileRepository{
		clock:   d.Clock(),
		client:  d.EtcdClient(),
		schema:  newFileSchema(d.EtcdSerde()),
		config:  config,
		backoff: backoff,
		all:     all,
	}
}

func (r *FileRepository) List(parentKey fmt.Stringer) iterator.DefinitionT[storage.File] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client)
}

func (r *FileRepository) Get(k storage.FileKey) op.ForType[*op.KeyValueT[storage.File]] {
	return r.get(k).WithEmptyResultAsError(func() error {
		return serviceError.NewResourceNotFoundError("file", k.String(), "sink")
	})
}

func (r *FileRepository) Create(sinkKey key.SinkKey, credentials *keboola.FileUploadCredentials) *op.AtomicOp[storage.File] {
	var sinkKV *op.KeyValueT[definition.Sink]
	var result storage.File
	return op.Atomic(r.client, &result).
		// Sink must exist
		ReadOp(r.all.sink.Get(sinkKey).WithResultTo(&sinkKV)).
		// Save
		WriteOrErr(func() (op op.Op, err error) {
			sink := sinkKV.Value

			// File should be created only for the table sinks
			if sink.Type != definition.SinkTypeTable {
				return nil, errors.Errorf(`unexpected sink type "%s", expected table sink`, sink.Type)
			}

			// Apply configuration patch from the sink to the global config
			cfg := r.config.With(sink.Table.Storage)

			// Create entity
			result, err = newFile(r.clock.Now(), cfg, sink.SinkKey, sink.Table.Mapping, credentials)
			if err != nil {
				return nil, err
			}

			// Save operation
			return r.put(result, true), nil
		})
}

func (r *FileRepository) IncrementRetry(k storage.FileKey, reason string) *op.AtomicOp[storage.File] {
	return r.update(k, func(slice storage.File) (storage.File, error) {
		slice.IncrementRetry(r.backoff, r.clock.Now(), reason)
		return slice, nil
	})
}

func (r *FileRepository) StateTransition(k storage.FileKey, to storage.FileState) *op.AtomicOp[storage.File] {
	now := r.clock.Now()
	return r.
		update(k, func(file storage.File) (storage.File, error) {
			if err := file.StateTransition(now, to); err != nil {
				return storage.File{}, err
			}
			return file, nil
		}).
		AddFrom(r.all.slice.onFileStateTransition(k, now, to))
}

func (r *FileRepository) Delete(k storage.FileKey) *op.TxnOp {
	txn := op.NewTxnOp(r.client)

	// Delete entity from All prefix
	txn.And(
		r.schema.
			AllLevels().ByKey(k).DeleteIfExists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("file", k.String(), "sink")
			}),
	)

	// Delete entity from InLevel prefixes
	for _, l := range storage.AllLevels() {
		txn.Then(r.schema.InLevel(l).ByKey(k).Delete(r.client))
	}

	// Delete all slices
	txn.And(r.all.slice.deleteAll(k))

	return txn
}

func (r *FileRepository) get(k storage.FileKey) op.ForType[*op.KeyValueT[storage.File]] {
	return r.schema.AllLevels().ByKey(k).Get(r.client)
}

// put saves the file.
// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
// - "All" prefix is used for classic CRUD operations.
// - "InLevel" prefix is used for effective watching of the storage level.
func (r *FileRepository) put(v storage.File, create bool) *op.TxnOp {
	level := v.State.Level()
	etcdKey := r.schema.AllLevels().ByKey(v.FileKey)

	txn := op.NewTxnOp(r.client)
	if create {
		// Entity must not exist on create
		txn.
			If(etcd.Compare(etcd.ModRevision(etcdKey.Key()), "=", 0)).
			AddProcessor(func(ctx context.Context, r *op.TxnResult) {
				if r.Err() == nil && !r.Succeeded() {
					r.AddErr(serviceError.NewResourceAlreadyExistsError("file", v.FileKey.String(), "sink"))
				}
			})
	}

	// Put entity to All and InLevel prefixes
	txn.Then(etcdKey.Put(r.client, v))
	txn.Then(r.schema.InLevel(level).ByKey(v.FileKey).Put(r.client, v))

	// Delete entity from other levels, if any
	// This simply handles the entity transition between different levels.
	for _, l := range storage.AllLevels() {
		if l != level {
			txn.Then(r.schema.InLevel(l).ByKey(v.FileKey).Delete(r.client))
		}
	}

	return txn
}

func (r *FileRepository) update(k storage.FileKey, updateFn func(storage.File) (storage.File, error)) *op.AtomicOp[storage.File] {
	var result storage.File
	var kv *op.KeyValueT[storage.File]
	return op.Atomic(r.client, &result).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&kv)).
		// Prepare the new value
		BeforeWriteOrErr(func() (err error) {
			result, err = updateFn(kv.Value)
			return err
		}).
		// Save the updated object
		Write(func() op.Op {
			return r.put(result, false)
		})
}

// newFile creates file definition.
func newFile(now time.Time, cfg storage.Config, sinkKey key.SinkKey, mapping definition.TableMapping, credentials *keboola.FileUploadCredentials) (f storage.File, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch cfg.Local.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return storage.File{}, errors.Errorf(`file compression type "%s" is not supported`, cfg.Local.Compression.Type)
	}

	// Convert path separator, on Windows
	fileKey := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(now)}}
	fileDir := filepath.FromSlash(fileKey.String()) //nolint:forbidigo

	f.FileKey = fileKey
	f.Type = storage.FileTypeCSV // different file types are not supported now
	f.State = storage.FileWriting
	f.Columns = mapping.Columns
	f.LocalStorage = local.NewFile(cfg.Local, fileDir)
	f.StagingStorage = staging.NewFile(cfg.Staging, f.LocalStorage, credentials)
	f.TargetStorage = target.NewFile(cfg.Target, mapping.TableID, f.StagingStorage)
	return f, nil
}
