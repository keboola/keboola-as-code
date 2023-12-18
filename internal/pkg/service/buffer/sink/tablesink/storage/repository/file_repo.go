package repository

import (
	"context"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	etcd "go.etcd.io/etcd/client/v3"
	"path/filepath"
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

func (r *FileRepository) ListInLevel(level storage.Level, parentKey fmt.Stringer) iterator.DefinitionT[storage.File] {
	return r.schema.InLevel(level).InObject(parentKey).GetAll(r.client)
}

func (r *FileRepository) Get(k storage.FileKey) op.ForType[storage.File] {
	return r.schema.AllLevels().ByKey(k).Get(r.client).WithEmptyResultAsError(func() error {
		return serviceError.NewResourceNotFoundError("file", k.String(), "sink")
	})
}

func (r *FileRepository) Create(fileKey storage.FileKey, credentials *keboola.FileUploadCredentials) *op.AtomicOp[storage.File] {
	var sink definition.Sink
	var oldLocalFiles []storage.File
	var result storage.File
	return op.Atomic(r.client, &result).
		// Get sink, it must exist
		ReadOp(r.all.sink.Get(fileKey.SinkKey).WithResultTo(&sink)).
		// There can be a maximum of one old file in the storage.FileWriting state,
		// if present, it is atomically switched to the storage.FileClosing state.
		ReadOp(r.ListInLevel(storage.LevelLocal, fileKey.SinkKey).WithResultTo(&oldLocalFiles)).
		WriteOrErr(func() (op.Op, error) {
			var count int
			var closeFileOp op.Op
			for _, oldFile := range oldLocalFiles {
				if oldFile.FileKey == fileKey {
					// File already exists
					return nil, serviceError.NewResourceAlreadyExistsError("file", fileKey.String(), "sink")
				}
				if oldFile.State == storage.FileWriting {
					modified := oldFile
					if err := modified.StateTransition(fileKey.OpenedAt().Time(), storage.FileClosing); err != nil {
						return nil, err
					}

					count++
					closeFileOp = r.update(oldFile, modified)
				}
			}

			if count > 1 {
				return nil, errors.Errorf(`unexpected state, found %d opened files in the sink "%s"`, count, fileKey.SinkKey)
			}

			return closeFileOp, nil
		}).
		// Save the new file
		WriteOrErr(func() (op op.Op, err error) {
			// File should be created only for the table sinks
			if sink.Type != definition.SinkTypeTable {
				return nil, errors.Errorf(`unexpected sink type "%s", expected table sink`, sink.Type)
			}

			// Apply configuration patch from the sink to the global config
			cfg := r.config.With(sink.Table.Storage)

			// Create entity
			result, err = newFile(fileKey, cfg, sink.Table.Mapping, credentials)
			if err != nil {
				return nil, err
			}

			// Save
			return r.create(result), nil
		})
}

func (r *FileRepository) IncrementRetry(k storage.FileKey, reason string) *op.AtomicOp[storage.File] {
	return r.readAndUpdate(k, func(slice storage.File) (storage.File, error) {
		slice.IncrementRetry(r.backoff, r.clock.Now(), reason)
		return slice, nil
	})
}

func (r *FileRepository) StateTransition(k storage.FileKey, to storage.FileState) *op.AtomicOp[storage.File] {
	now := r.clock.Now()
	return r.
		readAndUpdate(k, func(file storage.File) (storage.File, error) {
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

// create saves a new entity, see also update method.
// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
// - "All" prefix is used for classic CRUD operations.
// - "InLevel" prefix is used for effective watching of the storage level.
func (r *FileRepository) create(value storage.File) *op.TxnOp {
	etcdKey := r.schema.AllLevels().ByKey(value.FileKey)
	return op.NewTxnOp(r.client).
		// Entity must not exist on create
		If(etcd.Compare(etcd.ModRevision(etcdKey.Key()), "=", 0)).
		AddProcessor(func(ctx context.Context, r *op.TxnResult) {
			if r.Err() == nil && !r.Succeeded() {
				r.AddErr(serviceError.NewResourceAlreadyExistsError("file", value.FileKey.String(), "sink"))
			}
		}).
		// Put entity to All and InLevel prefixes
		Then(etcdKey.Put(r.client, value)).
		Then(r.schema.InLevel(value.State.Level()).ByKey(value.FileKey).Put(r.client, value))
}

// update saves an existing entity, see also create method.
func (r *FileRepository) update(oldValue, newValue storage.File) *op.TxnOp {
	txn := op.NewTxnOp(r.client)

	// Put entity to All and InLevel prefixes
	txn.
		Then(r.schema.AllLevels().ByKey(newValue.FileKey).Put(r.client, newValue)).
		Then(r.schema.InLevel(newValue.State.Level()).ByKey(newValue.FileKey).Put(r.client, newValue))

	// Delete entity from old level, if needed.
	if newValue.State.Level() != oldValue.State.Level() {
		txn.Then(r.schema.InLevel(oldValue.State.Level()).ByKey(oldValue.FileKey).Delete(r.client))
	}

	return txn
}

func (r *FileRepository) readAndUpdate(k storage.FileKey, updateFn func(storage.File) (storage.File, error)) *op.AtomicOp[storage.File] {
	var oldValue, newValue storage.File
	return op.Atomic(r.client, &newValue).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&oldValue)).
		// Prepare the new value
		BeforeWriteOrErr(func() (err error) {
			newValue, err = updateFn(oldValue)
			return err
		}).
		// Save the updated object
		Write(func() op.Op { return r.update(oldValue, newValue) })
}

// newFile creates file definition.
func newFile(fileKey storage.FileKey, cfg storage.Config, mapping definition.TableMapping, credentials *keboola.FileUploadCredentials) (f storage.File, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch cfg.Local.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return storage.File{}, errors.Errorf(`file compression type "%s" is not supported`, cfg.Local.Compression.Type)
	}

	// Convert path separator, on Windows
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
