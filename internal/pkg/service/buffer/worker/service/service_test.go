package service_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

const receiverSecret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

// createExport creates receiver,export,mapping,file and slice.
func createExport(t *testing.T, receiverID, exportID string, ctx context.Context, clk clock.Clock, client *etcd.Client, str *store.Store, fileRes *keboola.FileUploadCredentials) key.SliceKey {
	t.Helper()
	receiver := model.ReceiverForTest(receiverID, 0, clk.Now())
	columns := []column.Column{
		column.ID{Name: "col01"},
		column.Datetime{Name: "col02"},
		column.IP{Name: "col03"},
		column.Body{Name: "col04"},
		column.Headers{Name: "col05"},
		column.Template{Name: "col06", Language: "jsonnet", Content: `"---" + Body("key") + "---"`},
	}
	export := model.ExportForTest(receiver.ReceiverKey, exportID, "in.c-bucket.table", columns, clk.Now())

	if fileRes != nil {
		export.OpenedFile.StorageResource = fileRes
		export.OpenedSlice.StorageResource = fileRes
	}

	etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.CreateReceiver(ctx, receiver))
		assert.NoError(t, str.CreateExport(ctx, export))
	})
	return export.OpenedSlice.SliceKey
}

func createRecords(t *testing.T, ctx context.Context, clk *clock.Mock, d bufferDependencies.Mocked, key key.ReceiverKey, start, count int) {
	t.Helper()

	importer := receive.NewImporter(d)
	d.RequestHeaderMutable().Set("Content-Type", "application/json")
	for i := start; i < start+count; i++ {
		if clk != nil {
			clk.Add(time.Second)
		}
		body := io.NopCloser(strings.NewReader(fmt.Sprintf(`{"key":"value%03d"}`, i)))
		assert.NoError(t, importer.CreateRecord(ctx, d, key, receiverSecret, body))
	}
}

func createReceiverAndExportViaAPI(t *testing.T, d bufferDependencies.Mocked, api buffer.Service) (*buffer.Receiver, string, *buffer.Export) {
	t.Helper()
	task, err := api.CreateReceiver(d, &buffer.CreateReceiverPayload{
		Name: "my-receiver",
	})
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		task, err := api.GetTask(d, &buffer.GetTaskPayload{
			ReceiverID: task.ReceiverID,
			Type:       task.Type,
			TaskID:     task.ID,
		})
		assert.NoError(t, err)
		return task.IsFinished
	}, 10*time.Second, 100*time.Millisecond)

	receiver, err := api.GetReceiver(d, &buffer.GetReceiverPayload{
		ReceiverID: "my-receiver",
	})
	assert.NoError(t, err)

	task, err = api.CreateExport(d, &buffer.CreateExportPayload{
		ReceiverID: receiver.ID,
		Name:       "my-export",
		Mapping: &buffer.Mapping{
			TableID: "in.c-bucket.table",
			Columns: []*buffer.Column{
				{Name: "idCol", Type: "id", PrimaryKey: true},
				{Name: "dateCol", Type: "datetime", PrimaryKey: true},
				{Name: "bodyCol", Type: "body"},
			},
		},
		Conditions: &buffer.Conditions{
			Count: 10,
			Size:  "1MB",
			Time:  "1h",
		},
	})
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		task, err := api.GetTask(d, &buffer.GetTaskPayload{
			ReceiverID: task.ReceiverID,
			Type:       task.Type,
			TaskID:     task.ID,
		})
		assert.NoError(t, err)
		return task.IsFinished
	}, 10*time.Second, 100*time.Millisecond)

	export, err := api.GetExport(d, &buffer.GetExportPayload{
		ReceiverID: "my-receiver",
		ExportID:   "my-export",
	})
	assert.NoError(t, err)

	assert.NoError(t, err)
	secret := receiver.URL[strings.LastIndex(receiver.URL, "/")+1:]
	return receiver, secret, export
}
