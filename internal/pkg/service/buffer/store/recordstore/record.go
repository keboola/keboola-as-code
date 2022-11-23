package recordstore

import (
	"bytes"
	"context"
	"encoding/csv"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (c *Store) CreateRecord(ctx context.Context, recordKey model.RecordKey, csvData []string) (err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.CreateRecord")
	defer telemetry.EndSpan(span, &err)

	key := recordKey.Key()

	csvBuffer := new(bytes.Buffer)
	w := csv.NewWriter(csvBuffer)

	if err := w.WriteAll([][]string{csvData}); err != nil {
		return err
	}

	if err := w.Error(); err != nil {
		return err
	}

	_, err = client.KV.Put(ctx, key.Key(), csvBuffer.String())
	if err != nil {
		return err
	}

	return nil
}
