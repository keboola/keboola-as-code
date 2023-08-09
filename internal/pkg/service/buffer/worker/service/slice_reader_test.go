package service

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestRecordsReader(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	recordsCount := 5
	recordSize := datasize.MB
	value := idgenerator.Random(int(recordSize))

	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	logger := log.NewDebugLogger()
	sm := schema.New(validator.New().Validate)
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(clk.Now())}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now())}
	slice := model.Slice{
		SliceKey: sliceKey,
		IDRange:  &model.SliceIDRange{Start: 1, Count: uint64(recordsCount)},
	}
	receivedStats := statistics.Value{RecordsCount: uint64(recordsCount)}
	uploadStats := statistics.AfterUpload{}

	// Create records
	// t.Logf(`write start: %s`, time.Now())
	for i := 0; i < recordsCount; i++ {
		clk.Add(time.Second)
		recordKey := key.RecordKey{SliceKey: sliceKey, ReceivedAt: key.ReceivedAt(clk.Now()), RandomSuffix: "abcde"}
		assert.NoError(t, sm.Records().ByKey(recordKey).Put(value).Do(ctx, client))
	}
	// t.Logf(`write end: %s`, time.Now())

	// Read all
	// start := time.Now()
	// t.Logf(`read start: %s`, start)
	reader := newRecordsReader(ctx, logger, client, sm, slice, receivedStats, &uploadStats, 100)
	rSize, err := io.Copy(io.Discard, reader)
	assert.NoError(t, err)
	assert.Equal(t, (recordSize * datasize.ByteSize(recordsCount)).String(), datasize.ByteSize(rSize).String())
	// end := time.Now()
	// t.Logf(`read end: %s`, end)
	// t.Logf(`duration: %s`, end.Sub(start))
	// t.Logf(`size: %s`, datasize.ByteSize(rSize).HumanReadable())
}
