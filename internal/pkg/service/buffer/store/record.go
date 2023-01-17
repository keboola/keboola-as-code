package store

import (
	"context"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Store) CreateRecord(ctx context.Context, recordKey key.RecordKey, csvRow string) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.CreateRecord")
	defer telemetry.EndSpan(span, &err)

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

func (s *Store) CountRecords(ctx context.Context, k key.SliceKey) (count int64, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.RecordsCount")
	defer telemetry.EndSpan(span, &err)
	return s.schema.Records().InSlice(k).Count().Do(ctx, s.client)
}
