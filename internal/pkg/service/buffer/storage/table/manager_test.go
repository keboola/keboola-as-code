package table_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestManager_EnsureBucketExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := testproject.GetTestProjectForTest(t)
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithTestProject(p))
	m := NewManager(d.KeboolaProjectAPI())
	rb := rollback.New(d.Logger())
	client := p.KeboolaProjectAPI()

	bucketID := keboola.BucketID{
		Stage:      keboola.BucketStageIn,
		BucketName: "c-my-bucket",
	}

	// Create bucket
	assert.NoError(t, m.EnsureBucketExists(ctx, rb, bucketID))
	bucket, err := client.GetBucketRequest(bucketID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket", bucket.ID.String())

	// No operation, bucket exists
	assert.NoError(t, m.EnsureBucketExists(ctx, rb, bucketID))
	bucket, err = client.GetBucketRequest(bucketID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket", bucket.ID.String())

	// Test rollback, new bucket is deleted
	rb.Invoke(ctx)
	assert.Empty(t, d.DebugLogger().WarnMessages())
	_, err = client.GetBucketRequest(bucketID).Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, "storage.buckets.notFound", err.(*keboola.StorageError).ErrCode)
}

func TestManager_EnsureTableExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := testproject.GetTestProjectForTest(t)
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithTestProject(p))
	m := NewManager(d.KeboolaProjectAPI())
	rb := rollback.New(d.Logger())
	client := p.KeboolaProjectAPI()

	tableID := keboola.TableID{
		BucketID: keboola.BucketID{
			Stage:      keboola.BucketStageIn,
			BucketName: "c-my-bucket",
		},
		TableName: "my-table",
	}
	export := &model.Export{
		Mapping: model.Mapping{
			TableID: tableID,
			Columns: []column.Column{
				column.ID{Name: "foo"},
				column.Body{Name: "bar"},
			},
		},
	}

	// Create bucket
	assert.NoError(t, m.EnsureBucketExists(ctx, rb, tableID.BucketID))

	// Create table
	assert.NoError(t, m.EnsureTableExists(ctx, rb, export))
	table, err := client.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket.my-table", table.ID.String())

	// No operation, table exists
	assert.NoError(t, m.EnsureTableExists(ctx, rb, export))
	table, err = client.GetTableRequest(tableID).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket.my-table", table.ID.String())

	// Primary key does not match
	export.Mapping.Columns[0] = column.ID{
		Name:       export.Mapping.Columns[0].ColumnName(),
		PrimaryKey: true,
	}
	err = m.EnsureTableExists(ctx, rb, export)
	assert.Error(t, err)
	assert.Equal(t, `primary key of the table "in.c-my-bucket.my-table" does not match expected ["foo"], found []`, err.Error())

	// Columns do not match
	export.Mapping.Columns = []column.Column{
		column.ID{Name: "different"},
		column.Body{Name: "columns"},
	}
	err = m.EnsureTableExists(ctx, rb, export)
	assert.Error(t, err)
	assert.Equal(t, `columns of the table "in.c-my-bucket.my-table" do not match expected ["different","columns"], found ["foo","bar"]`, err.Error())

	// Test rollback, new bucket and table are deleted
	rb.Invoke(ctx)
	assert.Empty(t, d.DebugLogger().WarnMessages())
	_, err = client.GetTableRequest(tableID).Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, "storage.tables.notFound", err.(*keboola.StorageError).ErrCode)
	_, err = client.GetBucketRequest(tableID.BucketID).Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, "storage.buckets.notFound", err.(*keboola.StorageError).ErrCode)
}
