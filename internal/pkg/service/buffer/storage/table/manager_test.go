package table_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

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
	d := dependencies.NewMockedDeps(t, dependencies.WithTestProject(p))
	m := NewManager(d)
	rb := rollback.New(d.Logger())
	client := p.StorageAPIClient()

	bucketID := storageapi.BucketID{
		Stage:      storageapi.BucketStageIn,
		BucketName: "my-bucket",
	}

	// Create bucket
	assert.NoError(t, m.EnsureBucketExists(ctx, rb, bucketID))
	bucket, err := storageapi.GetBucketRequest(bucketID).Send(ctx, p.StorageAPIClient())
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket", bucket.ID.String())

	// No operation, bucket exists
	assert.NoError(t, m.EnsureBucketExists(ctx, rb, bucketID))
	bucket, err = storageapi.GetBucketRequest(bucketID).Send(ctx, p.StorageAPIClient())
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket", bucket.ID.String())

	// Test rollback, new bucket is deleted
	rb.Invoke(ctx)
	assert.Empty(t, d.DebugLogger().WarnMessages())
	_, err = storageapi.GetBucketRequest(bucketID).Send(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, "storage.buckets.notFound", err.(*storageapi.Error).ErrCode)
}

func TestManager_EnsureTableExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := testproject.GetTestProjectForTest(t)
	d := dependencies.NewMockedDeps(t, dependencies.WithTestProject(p))
	m := NewManager(d)
	rb := rollback.New(d.Logger())
	client := p.StorageAPIClient()

	tableID := storageapi.TableID{
		BucketID: storageapi.BucketID{
			Stage:      storageapi.BucketStageIn,
			BucketName: "my-bucket",
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
	table, err := storageapi.GetTableRequest(tableID).Send(ctx, p.StorageAPIClient())
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket.my-table", table.ID.String())

	// No operation, table exists
	assert.NoError(t, m.EnsureTableExists(ctx, rb, export))
	table, err = storageapi.GetTableRequest(tableID).Send(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, "in.c-my-bucket.my-table", table.ID.String())

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
	_, err = storageapi.GetTableRequest(tableID).Send(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, "storage.tables.notFound", err.(*storageapi.Error).ErrCode)
	_, err = storageapi.GetBucketRequest(tableID.BucketID).Send(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, "storage.buckets.notFound", err.(*storageapi.Error).ErrCode)
}
