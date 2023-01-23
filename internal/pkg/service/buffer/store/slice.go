package store

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Store) CreateSlice(ctx context.Context, slice model.Slice) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.CreateSlice")
	defer telemetry.EndSpan(span, &err)

	_, err = s.createSliceOp(ctx, slice).Do(ctx, s.client)
	return err
}

func (s *Store) createSliceOp(_ context.Context, slice model.Slice) op.BoolOp {
	return s.schema.
		Slices().
		Opened().
		ByKey(slice.SliceKey).
		PutIfNotExists(slice).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("slice", slice.SliceID.String(), "file")
			}
			return ok, err
		})
}

func (s *Store) GetSlice(ctx context.Context, sliceKey key.SliceKey) (r model.Slice, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetSlice")
	defer telemetry.EndSpan(span, &err)

	slice, err := s.getSliceOp(ctx, sliceKey).Do(ctx, s.client)
	if err != nil {
		return model.Slice{}, err
	}
	return slice.Value, nil
}

func (s *Store) getSliceOp(_ context.Context, sliceKey key.SliceKey) op.ForType[*op.KeyValueT[model.Slice]] {
	return s.schema.
		Slices().
		Opened().
		ByKey(sliceKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Slice], err error) (*op.KeyValueT[model.Slice], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("slice", sliceKey.SliceID.String(), "file")
			}
			return kv, err
		})
}

func (s *Store) getOpenedSliceOp(_ context.Context, exportKey key.ExportKey, opts ...etcd.OpOption) op.ForType[*op.KeyValueT[model.Slice]] {
	opts = append(opts, etcd.WithSort(etcd.SortByKey, etcd.SortDescend))
	return s.schema.
		Slices().
		Opened().
		InExport(exportKey).
		GetOne(opts...).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Slice], err error) (*op.KeyValueT[model.Slice], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewNoResourceFoundError("opened slice", "export")
			}
			return kv, err
		})
}

func (s *Store) ListUploadedSlices(ctx context.Context, fileKey key.FileKey) (r []model.Slice, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetAllUploadedSlices")
	defer telemetry.EndSpan(span, &err)

	slices, err := s.listUploadedSlicesOp(ctx, fileKey).Do(ctx, s.client).All()
	if err != nil {
		return nil, err
	}

	return slices.Values(), nil
}

func (s *Store) listUploadedSlicesOp(_ context.Context, fileKey key.FileKey) iterator.DefinitionT[model.Slice] {
	return s.schema.Slices().Uploaded().InFile(fileKey).GetAll()
}

func (s *Store) CloseSlice(ctx context.Context, slice *model.Slice) (err error) {
	k := slice.SliceKey
	statsPfx := s.schema.ReceivedStats().InSlice(k)
	var recordsCount uint64
	var recordLastID uint64
	var stats model.Stats
	return op.
		Atomic().
		Read(func() op.Op {
			return op.MergeToTxn(
				assertAllPrevSlicesClosed(s.schema, k),
				sumStatsOp(statsPfx.PrefixT().GetAll(), &stats),
				s.countRecordsOp(k, &recordsCount),
				s.loadExportRecordsCounter(k.ExportKey, &recordLastID),
			)
		}).
		WriteOrErr(func() (op.Op, error) {
			var ops []op.Op

			// Copy slice and do modifications
			modSlice := *slice

			// Set statistics to the slice.
			// The records count from the statistics may not be accurate
			// if some statistics were not sent due to a network error.
			if recordsCount > 0 {
				modSlice.Statistics = &stats
				modSlice.Statistics.RecordsCount = recordsCount
			} else {
				modSlice.IsEmpty = true
			}

			// Delete all "per node" statistics
			ops = append(ops, statsPfx.DeleteAll())

			// Set ID range to the slice and update counter
			if recordsCount > 0 {
				modSlice.IDRange = &model.SliceIDRange{
					Start: recordLastID + 1,
					Count: recordsCount,
				}
				ops = append(ops, s.setExportRecordsCounterOp(k.ExportKey, recordLastID+recordsCount))
			}

			// Set slice state from "closing" to "uploading"
			// This also saves the changes.
			if v, err := s.setSliceStateOp(ctx, s.clock.Now(), &modSlice, slicestate.Uploading); err != nil {
				return nil, err
			} else {
				ops = append(ops, v)
			}

			return op.
				MergeToTxn(ops...).
				WithOnResult(func(result op.TxnResult) {
					*slice = modSlice
				}), nil
		}).
		Do(ctx, s.client)
}

// MarkSliceUploaded when the upload is finished.
func (s *Store) MarkSliceUploaded(ctx context.Context, slice *model.Slice) error {
	setOp, err := s.setSliceStateOp(ctx, s.clock.Now(), slice, slicestate.Uploaded)
	if err != nil {
		return err
	}

	return op.MergeToTxn(
		// Modify slice state "uploading" -> "uploaded".
		setOp,
		// Remove uploaded records
		s.schema.Records().InSlice(slice.SliceKey).DeleteAll(),
	).DoOrErr(ctx, s.client)
}

// MarkSliceUploadFailed when the upload failed.
func (s *Store) MarkSliceUploadFailed(ctx context.Context, slice *model.Slice) error {
	setOp, err := s.setSliceStateOp(ctx, s.clock.Now(), slice, slicestate.Failed)
	if err != nil {
		return err
	}
	return setOp.DoOrErr(ctx, s.client)
}

// ScheduleSliceForRetry when it is time for the next upload attempt.
func (s *Store) ScheduleSliceForRetry(ctx context.Context, slice *model.Slice) error {
	setOp, err := s.setSliceStateOp(ctx, s.clock.Now(), slice, slicestate.Uploading)
	if err != nil {
		return err
	}
	return setOp.DoOrErr(ctx, s.client)
}

// SetSliceState method atomically changes the state of the file.
// False is returned, if the given file is already in the target state.
func (s *Store) SetSliceState(ctx context.Context, slice *model.Slice, to slicestate.State) (err error) { //nolint:dupl
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.SetSliceState")
	defer telemetry.EndSpan(span, &err)

	txn, err := s.setSliceStateOp(ctx, s.clock.Now(), slice, to)
	if err != nil {
		return err
	}

	_, err = txn.Do(ctx, s.client)
	return err
}

// SwapSlice closes the old slice and creates the new one, in the same export.
func (s *Store) SwapSlice(ctx context.Context, oldSlice *model.Slice) (newSlice model.Slice, err error) {
	now := s.clock.Now()
	newSlice = model.NewSlice(oldSlice.FileKey, now, oldSlice.Mapping, oldSlice.Number+1, oldSlice.StorageResource)
	swapOp, err := s.swapSliceOp(ctx, now, oldSlice, newSlice)
	if err != nil {
		return model.Slice{}, err
	}
	if err := swapOp.DoOrErr(ctx, s.client); err != nil {
		return model.Slice{}, err
	}
	return newSlice, err
}

// swapSliceOp closes the old slice and creates the new one, in the same export.
func (s *Store) swapSliceOp(ctx context.Context, now time.Time, oldSlice *model.Slice, newSlice model.Slice) (op.Op, error) {
	if newSlice.ExportKey != oldSlice.ExportKey {
		panic(errors.Errorf(`new slice "%s" is not from the export "%s"`, newSlice.SliceKey, oldSlice.ExportKey))
	}
	createSliceOp := s.createSliceOp(ctx, newSlice)
	closeSliceOp, err := s.setSliceStateOp(ctx, now, oldSlice, slicestate.Closing)
	if err != nil {
		return nil, err
	}
	return op.MergeToTxn(createSliceOp, closeSliceOp), nil
}

func (s *Store) setSliceStateOp(ctx context.Context, now time.Time, slice *model.Slice, to slicestate.State) (*op.TxnOpDef, error) { //nolint:dupl
	from := slice.State
	clone := *slice
	stm := slicestate.NewSTM(slice.State, func(ctx context.Context, from, to slicestate.State) error {
		// Update fields
		nowUTC := model.UTCTime(now)
		clone.State = to
		switch to {
		case slicestate.Closing:
			clone.ClosingAt = &nowUTC
		case slicestate.Uploading:
			clone.UploadingAt = &nowUTC
		case slicestate.Uploaded:
			clone.UploadedAt = &nowUTC
		case slicestate.Failed:
			clone.FailedAt = &nowUTC
		case slicestate.Imported:
			clone.ImportedAt = &nowUTC
		default:
			panic(errors.Errorf(`unexpected state "%s"`, to))
		}
		return nil
	})

	if err := stm.To(ctx, to); err != nil {
		return nil, err
	}

	// Atomically swap keys in the transaction
	return op.
		MergeToTxn(
			s.schema.Slices().InState(from).ByKey(slice.SliceKey).DeleteIfExists(),
			s.schema.Slices().InState(to).ByKey(slice.SliceKey).PutIfNotExists(clone),
		).
		WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, result op.TxnResult, err error) error {
			if err == nil {
				*slice = clone
				if !result.Succeeded {
					return errors.Errorf(`slice "%s" is already in the "uploading" state`, slice.SliceKey)
				}
			}
			return err
		}), nil
}

// assertAllPrevSlicesClosed checks there is no other previous slice that is not closed,
// So the ID range for slice records can be generated.
func assertAllPrevSlicesClosed(schema *schema.Schema, k key.SliceKey) op.Op {
	return op.MergeToTxn(
		assertNoPreviousSliceInState(schema, k, slicestate.Opened),
		assertNoPreviousSliceInState(schema, k, slicestate.Closing),
	)
}

func assertNoPreviousSliceInState(schema *schema.Schema, k key.SliceKey, state slicestate.State) op.Op {
	prefix := schema.Slices().InState(state)
	end := etcd.WithRange(prefix.ByKey(k).Key())
	return prefix.InExport(k.ExportKey).Count(end).WithOnResultOrErr(func(v int64) error {
		if v > 0 {
			return errors.Errorf(`no slice in the state "%s" expected before the "%s", found %v`, state, k, v)
		}
		return nil
	})
}
