package service_test

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
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

func createRecords(t *testing.T, ctx context.Context, clk *clock.Mock, d bufferDependencies.APIScope, key key.ReceiverKey, start, count int) {
	t.Helper()

	importer := receive.NewImporter(d)
	for i := start; i < start+count; i++ {
		if clk != nil {
			clk.Add(time.Second)
		}
		body := io.NopCloser(strings.NewReader(fmt.Sprintf(`{"key":"value%03d"}`, i)))
		req := httptest.NewRequest("GET", "/foo", body)
		req.RemoteAddr = "1.2.3.4:789"
		req.Header.Set("Content-Type", "application/json")
		reqInfo := dependencies.NewRequestInfo(req)
		assert.NoError(t, importer.CreateRecord(ctx, reqInfo, key, receiverSecret, body))
	}
}
