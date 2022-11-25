package recordstore

import (
	"bytes"
	"context"
	"encoding/csv"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/schema"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (c *Store) CreateRecord(ctx context.Context, recordKey schema.RecordKey, csvData []string) (err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.recordstore.CreateRecord")
	defer telemetry.EndSpan(span, &err)

	key := recordKey.In(c.schema)
	csvBuffer := new(bytes.Buffer)
	w := csv.NewWriter(csvBuffer)

	if err := w.WriteAll([][]string{csvData}); err != nil {
		return err
	}

	if err := w.Error(); err != nil {
		return err
	}

	csvString := csvBuffer.String()
	if datasize.ByteSize(len(csvString)) > MaxMappedCSVRowSizeInBytes {
		return serviceError.NewPayloadTooLargeError(errors.Errorf(
			"size of the mapped csv row exceeded the limit of %s.",
			MaxMappedCSVRowSizeInBytes,
		))
	}

	_, err = client.KV.Put(ctx, key.Key(), csvString)
	return err
}
