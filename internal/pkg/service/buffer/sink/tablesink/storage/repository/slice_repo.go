package repository

import (
	"context"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	etcd "go.etcd.io/etcd/client/v3"
	"path/filepath"
	"reflect"
	"time"
)

type SliceRepository struct {
	clock   clock.Clock
	client  etcd.KV
	schema  sliceSchema
	backoff storage.RetryBackoff
	all     *Repository
}

func newSliceRepository(d dependencies, backoff storage.RetryBackoff, all *Repository) *SliceRepository {
	return &SliceRepository{
		clock:   d.Clock(),
		client:  d.EtcdClient(),
		schema:  newSliceSchema(d.EtcdSerde()),
		backoff: backoff,
		all:     all,
	}
}

func (r *SliceRepository) List(parentKey fmt.Stringer) iterator.DefinitionT[storage.Slice] {
	return r.schema.AllLevels().InObject(parentKey).GetAll(r.client)
}

func (r *SliceRepository) Get(k storage.SliceKey) op.ForType[*op.KeyValueT[storage.Slice]] {
	return r.schema.
		AllLevels().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("slice", k.String(), "file")
		})
}

func (r *SliceRepository) Create(fileKey storage.FileKey, volumeID storage.VolumeID, prevSliceSize datasize.ByteSize) *op.AtomicOp[storage.Slice] {
	var fileKV *op.KeyValueT[storage.File]
	var result storage.Slice

	// Save the slice
	return op.Atomic(r.client, &result).
		// Sink must exist
		ReadOp(r.all.sink.ExistsOrErr(fileKey.SinkKey)).
		// File must exist and be in the FileWriting state
		ReadOp(r.all.file.get(fileKey).WithResultTo(&fileKV)).
		BeforeWriteOrErr(func() error {
			if fileKV == nil {
				return serviceError.NewResourceNotFoundError("file", fileKey.String(), "sink")
			} else if fileState := fileKV.Value.State; fileState != storage.FileWriting {
				return serviceError.NewBadRequestError(errors.Errorf(
					`slice cannot be created: unexpected file "%s" state "%s", expected "%s"`,
					fileKey.String(), fileState, storage.FileWriting,
				))
			} else {
				return nil
			}
		}).
		// Save
		WriteOrErr(func() (op op.Op, err error) {
			// Create entity
			result, err = newSlice(r.clock.Now(), fileKV.Value, volumeID, prevSliceSize)
			if err != nil {
				return nil, err
			}

			// Save operation
			return r.put(result, result, true), nil
		})
}

func (r *SliceRepository) IncrementRetry(k storage.SliceKey, reason string) *op.AtomicOp[storage.Slice] {
	return r.update(k, func(slice storage.Slice) (storage.Slice, error) {
		slice.IncrementRetry(r.backoff, r.clock.Now(), reason)
		return slice, nil
	})
}

func (r *SliceRepository) StateTransition(k storage.SliceKey, to storage.SliceState) *op.AtomicOp[storage.Slice] {
	var file *op.KeyValueT[storage.File]
	return r.
		update(k, func(slice storage.Slice) (storage.Slice, error) {
			// Validate file and slice state combination
			if err := validateFileAndSliceStates(file.Value.State, to); err != nil {
				return slice, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
			}

			// Switch slice state
			if err := slice.StateTransition(r.clock.Now(), to); err != nil {
				return storage.Slice{}, err
			}

			return slice, nil
		}).
		ReadOp(r.all.file.Get(k.FileKey).WithResultTo(&file))
}

func (r *SliceRepository) Delete(k storage.SliceKey) *op.TxnOp {
	txn := op.NewTxnOp(r.client)

	// Delete entity from All prefix
	txn.And(
		r.schema.
			AllLevels().ByKey(k).DeleteIfExists(r.client).
			WithEmptyResultAsError(func() error {
				return serviceError.NewResourceNotFoundError("slice", k.String(), "file")
			}),
	)

	// Delete entity from InLevel prefixes
	for _, l := range storage.AllLevels() {
		txn.Then(r.schema.InLevel(l).ByKey(k).Delete(r.client))
	}

	return txn
}

func (r *SliceRepository) deleteAll(parentKey fmt.Stringer) *op.TxnOp {
	txn := op.NewTxnOp(r.client)

	// Delete entity from All prefix
	txn.Then(r.schema.AllLevels().InObject(parentKey).DeleteAll(r.client))

	// Delete entity from InLevel prefixes
	for _, l := range storage.AllLevels() {
		txn.Then(r.schema.InLevel(l).InObject(parentKey).DeleteAll(r.client))
	}

	return txn
}

// put saves the slice.
// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
// - "All" prefix is used for classic CRUD operations.
// - "InLevel" prefix is used for effective watching of the storage level.
func (r *SliceRepository) put(oldValue, newValue storage.Slice, create bool) *op.TxnOp {
	level := newValue.State.Level()
	etcdKey := r.schema.AllLevels().ByKey(newValue.SliceKey)

	txn := op.NewTxnOp(r.client)
	if create {
		// Entity must not exist on create
		txn.
			If(etcd.Compare(etcd.ModRevision(etcdKey.Key()), "=", 0)).
			AddProcessor(func(ctx context.Context, r *op.TxnResult) {
				if r.Err() == nil && !r.Succeeded() {
					r.AddErr(serviceError.NewResourceAlreadyExistsError("slice", newValue.SliceKey.String(), "file"))
				}
			})
	}

	// Save entity to All prefix
	txn.Then(etcdKey.Put(r.client, newValue))

	// Save entity to new level.
	txn.Then(r.schema.InLevel(level).ByKey(newValue.SliceKey).Put(r.client, newValue))

	// Delete entity from old level, if needed.
	if !create && newValue.State.Level() != oldValue.State.Level() {
		txn.Then(r.schema.InLevel(oldValue.State.Level()).ByKey(oldValue.SliceKey).Delete(r.client))
	}

	return txn
}

func (r *SliceRepository) onFileStateTransition(k storage.FileKey, now time.Time, newFileState storage.FileState) *op.AtomicOp[op.NoResult] {
	// Validate and modify slice state
	return r.updateAllInFile(k, func(slice storage.Slice) (storage.Slice, error) {
		if newFileState == storage.FileClosing && slice.State == storage.SliceWriting {
			// Switch slice state on FileClosing
			if err := slice.StateTransition(now, storage.SliceClosing); err != nil {
				return slice, err
			}
		} else if newFileState == storage.FileImported && slice.State == storage.SliceUploaded {
			// Switch slice state on FileImported
			if err := slice.StateTransition(now, storage.SliceImported); err != nil {
				return slice, err
			}
		}

		// Validate file and slice state combination
		if err := validateFileAndSliceStates(newFileState, slice.State); err != nil {
			return slice, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
		}

		return slice, nil
	})
}

func (r *SliceRepository) update(k storage.SliceKey, updateFn func(slice storage.Slice) (storage.Slice, error)) *op.AtomicOp[storage.Slice] {
	var oldValue, newValue storage.Slice
	var kv *op.KeyValueT[storage.Slice]
	return op.Atomic(r.client, &newValue).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&kv)).
		// Prepare the new value
		BeforeWriteOrErr(func() (err error) {
			oldValue = kv.Value
			newValue, err = updateFn(oldValue)
			return err
		}).
		// Save the updated object
		Write(func() op.Op {
			return r.put(oldValue, newValue, false)
		})
}

// updateAllInFile updates all slices in a file.
func (r *SliceRepository) updateAllInFile(parentKey storage.FileKey, updateFn func(slice storage.Slice) (storage.Slice, error)) *op.AtomicOp[op.NoResult] {
	var original []storage.Slice
	return op.Atomic(r.client, &op.NoResult{}).
		// Read entities for modification
		ReadOp(r.List(parentKey).WithResultTo(&original)).
		// Modify and save entities
		WriteOrErr(func() (op.Op, error) {
			txn := op.NewTxnOp(r.client)
			errs := errors.NewMultiError()
			for _, oldValue := range original {
				if newValue, err := updateFn(oldValue); err == nil {
					// Save modified value, if here is a difference
					if !reflect.DeepEqual(newValue, oldValue) {
						txn.Then(r.put(oldValue, newValue, false))
					}
				} else {
					errs.Append(err)
				}
			}
			if err := errs.ErrorOrNil(); err != nil {
				return nil, err
			}
			if !txn.Empty() {
				return txn, nil
			}
			return nil, nil
		})
}

// newSlice creates slice definition.
func newSlice(now time.Time, file storage.File, volumeID storage.VolumeID, prevSliceSize datasize.ByteSize) (s storage.Slice, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch file.LocalStorage.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return storage.Slice{}, errors.Errorf(`file compression type "%s" is not supported`, file.LocalStorage.Compression.Type)
	}

	// Convert path separator, on Windows
	sliceKey := storage.SliceKey{FileKey: file.FileKey, SliceID: storage.SliceID{VolumeID: volumeID, OpenedAt: utctime.From(now)}}
	sliceDir := filepath.FromSlash(sliceKey.SliceID.OpenedAt.String()) //nolint: forbidigo

	// Generate unique staging storage path
	stagingPath := fmt.Sprintf(`%s_%s`, sliceKey.OpenedAt().String(), sliceKey.VolumeID)

	s.SliceKey = sliceKey
	s.Type = file.Type
	s.State = storage.SliceWriting
	s.Columns = file.Columns
	if s.LocalStorage, err = file.LocalStorage.NewSlice(sliceDir, prevSliceSize); err != nil {
		return storage.Slice{}, err
	}
	if s.StagingStorage, err = file.StagingStorage.NewSlice(stagingPath, s.LocalStorage); err != nil {
		return storage.Slice{}, err
	}
	return s, nil
}
