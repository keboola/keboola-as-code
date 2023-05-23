package store

import (
	"context"
	"strconv"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Store) CreateRecord(ctx context.Context, recordKey key.RecordKey, csvRow string) (err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.buffer.store.CreateRecord")
	defer span.End(&err)

	if recordKey.RandomSuffix == "" {
		recordKey.RandomSuffix = idgenerator.Random(5)
	} else {
		return errors.New("unexpected state: record random suffix should by empty, it is generated on record create")
	}

	// Check size
	if datasize.ByteSize(len(csvRow)) > MaxMappedCSVRowSizeInBytes {
		return serviceError.NewPayloadTooLargeError(errors.Errorf(
			"size of the mapped csv row exceeded the limit of %s.",
			MaxMappedCSVRowSizeInBytes,
		))
	}

	return s.schema.Records().ByKey(recordKey).Put(csvRow).Do(ctx, s.client)
}

func (s *Store) CountRecords(ctx context.Context, k key.SliceKey) (count uint64, err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.buffer.store.RecordsCount")
	defer span.End(&err)
	err = s.countRecordsOp(k, &count).DoOrErr(ctx, s.client)
	return count, err
}

func (s *Store) countRecordsOp(k key.SliceKey, out *uint64) op.Op {
	return s.schema.Records().InSlice(k).Count().WithOnResult(func(v int64) {
		*out = uint64(v)
	})
}

func (s *Store) setExportRecordsCounterOp(k key.ExportKey, newValue uint64) op.Op {
	counterKey := s.schema.Runtime().LastRecordID().ByKey(k)
	return counterKey.Put(strconv.FormatUint(newValue, 10))
}

func (s *Store) loadExportRecordsCounter(k key.ExportKey, out *uint64) op.Op {
	counterKey := s.schema.Runtime().LastRecordID().ByKey(k)
	return counterKey.Get().
		WithOnResultOrErr(func(kv *op.KeyValue) error {
			if kv == nil {
				// Counter key is missing, use zero
				*out = 0
				return nil
			}

			// Key has been found, parse it
			parsed, err := strconv.ParseUint(string(kv.Value), 10, 64)
			if err != nil {
				return err
			}
			*out = parsed
			return nil
		})
}
