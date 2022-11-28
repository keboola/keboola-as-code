package store

import (
	"bytes"
	"context"
	"encoding/csv"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Store) CreateRecord(ctx context.Context, recordKey key.RecordKey, csvData []string) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.recordstore.CreateRecord")
	defer telemetry.EndSpan(span, &err)

	if recordKey.RandomSuffix != "" {
		return errors.New("unexpected state: record random suffix should by empty, it is generated on record create")
	}
	recordKey.RandomSuffix = idgenerator.Random(5)

	// Convert columns to CSV
	csvBuffer := new(bytes.Buffer)
	w := csv.NewWriter(csvBuffer)
	if err := w.WriteAll([][]string{csvData}); err != nil {
		return err
	}
	if err := w.Error(); err != nil {
		return err
	}

	// Check size
	csvString := csvBuffer.String()
	if datasize.ByteSize(len(csvString)) > MaxMappedCSVRowSizeInBytes {
		return serviceError.NewPayloadTooLargeError(errors.Errorf(
			"size of the mapped csv row exceeded the limit of %s.",
			MaxMappedCSVRowSizeInBytes,
		))
	}

	return s.schema.Records().ByKey(recordKey).Put(csvString).Do(ctx, s.client)
}
