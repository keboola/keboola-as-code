package file_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestManager_CreateFile(t *testing.T) {
	t.Parallel()

	now, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	clk := clock.NewMock()
	clk.Set(now)

	ctx := context.Background()
	p := testproject.GetTestProjectForTest(t)
	d := dependencies.NewMocked(t, dependencies.WithClock(clk), dependencies.WithTestProject(p))
	m := NewManager(d.Clock(), d.KeboolaProjectAPI(), nil)
	rb := rollback.New(d.Logger())
	client := p.KeboolaProjectAPI()

	receiverKey := key.ReceiverKey{ProjectID: keboola.ProjectID(123), ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	export := model.Export{
		ExportBase: model.ExportBase{
			ExportKey: exportKey,
		},
		Mapping: model.Mapping{
			MappingKey: key.MappingKey{ExportKey: exportKey, RevisionID: 1},
			TableID:    keboola.MustParseTableID("in.c-bucket.table"),
			Columns: []column.Column{
				column.ID{Name: "id"},
			},
		},
	}

	// Create file for the export
	assert.NoError(t, m.CreateFileForExport(ctx, rb, &export))
	assert.NotEmpty(t, export.OpenedFile.StorageResource.ID)
	assert.Equal(t, "my_receiver_my_export_20060101080405", export.OpenedFile.StorageResource.Name)

	// Check file exists
	_, err := client.GetFileRequest(export.OpenedFile.StorageResource.ID).Send(ctx)
	assert.NoError(t, err)

	// Test rollback
	rb.Invoke(ctx)
	assert.Empty(t, d.DebugLogger().WarnMessages())
	_, err = client.GetFileRequest(export.OpenedFile.StorageResource.ID).Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, "storage.files.notFound", err.(*keboola.StorageError).ErrCode)
}
