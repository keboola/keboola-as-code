package upload_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

// createExport creates receiver,export,mapping,file and slice.
func createExport(t *testing.T, ctx context.Context, clk clock.Clock, client *etcd.Client, str *store.Store) key.SliceKey {
	t.Helper()
	receiver := model.ReceiverForTest("my-receiver", 0, clk.Now())
	columns := []column.Column{
		column.ID{Name: "col01"},
		column.Datetime{Name: "col02"},
		column.IP{Name: "col03"},
		column.Body{Name: "col04"},
		column.Headers{Name: "col05"},
		column.Template{Name: "col06", Language: "jsonnet", Content: `"---" + Body("key1") + "---"`},
	}
	export := model.ExportForTest(receiver.ReceiverKey, "my-export", "in.c-bucket.table", columns, clk.Now())
	etcdhelper.ExpectModification(t, client, func() {
		assert.NoError(t, str.CreateReceiver(ctx, receiver))
		assert.NoError(t, str.CreateExport(ctx, export))
	})
	return export.OpenedSlice.SliceKey
}
