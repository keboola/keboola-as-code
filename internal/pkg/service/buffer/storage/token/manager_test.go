package token_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/token"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestManager_CreateToken(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := testproject.GetTestProjectForTest(t)
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithTestProject(p))
	m := NewManager(d)
	rb := rollback.New(d.Logger())
	client := p.StorageAPIClient()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(123), ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	tableID := storageapi.MustParseTableID("in.c-bucket.table")
	export := model.Export{
		ExportBase: model.ExportBase{
			ExportKey: exportKey,
		},
		Mapping: model.Mapping{
			TableID: tableID,
			Columns: []column.Column{
				column.ID{Name: "id"},
			},
		},
	}

	// Create bucket
	_, err := storageapi.CreateBucketRequest(&storageapi.Bucket{ID: tableID.BucketID}).Send(ctx, client)
	assert.NoError(t, err)

	// Create token for the export
	assert.NoError(t, m.CreateToken(ctx, rb, &export))

	// Check token exists
	_, err = storageapi.VerifyTokenRequest(export.Token.Token).Send(ctx, client)
	assert.NoError(t, err)

	// Test rollback
	rb.Invoke(ctx)
	assert.Empty(t, d.DebugLogger().WarnMessages())
	_, err = storageapi.VerifyTokenRequest(export.Token.Token).Send(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, "storage.tokenInvalid", err.(*storageapi.Error).ErrCode)
}

func TestManager_RefreshToken_TokenExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := testproject.GetTestProjectForTest(t)
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithTestProject(p))
	m := NewManager(d)
	rb := rollback.New(d.Logger())
	client := p.StorageAPIClient()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(123), ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	tableID := storageapi.MustParseTableID("in.c-bucket.table")
	export := model.Export{
		ExportBase: model.ExportBase{
			ExportKey: exportKey,
		},
		Mapping: model.Mapping{
			TableID: tableID,
			Columns: []column.Column{
				column.ID{Name: "id"},
			},
		},
	}

	// Create bucket
	_, err := storageapi.CreateBucketRequest(&storageapi.Bucket{ID: tableID.BucketID}).Send(ctx, client)
	assert.NoError(t, err)

	// Create token for the export
	assert.NoError(t, m.CreateToken(ctx, rollback.New(d.Logger()), &export))
	oldToken := export.Token

	// Refresh token
	assert.NoError(t, m.RefreshToken(ctx, rb, &export.Token))

	// Token exists
	_, err = storageapi.VerifyTokenRequest(export.Token.Token).Send(ctx, client)
	assert.NoError(t, err)

	// Token differs
	assert.NotEqual(t, oldToken.Token, export.Token.Token)

	// Test rollback - no operation
	rb.Invoke(ctx)
	assert.Empty(t, d.DebugLogger().WarnMessages())
	_, err = storageapi.VerifyTokenRequest(export.Token.Token).Send(ctx, client)
	assert.NoError(t, err)
}

func TestManager_RefreshToken_TokenMissing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := testproject.GetTestProjectForTest(t)
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithTestProject(p))
	m := NewManager(d)
	rb := rollback.New(d.Logger())
	client := p.StorageAPIClient()

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(123), ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	tableID := storageapi.MustParseTableID("in.c-bucket.table")
	token := model.Token{
		ExportKey: exportKey,
		StorageToken: storageapi.Token{
			ID:    "1",
			Token: "some-missing-token",
		},
	}

	// Create bucket
	_, err := storageapi.CreateBucketRequest(&storageapi.Bucket{ID: tableID.BucketID}).Send(ctx, client)
	assert.NoError(t, err)

	// Refresh token
	assert.NoError(t, m.RefreshToken(ctx, rb, &token))

	// Token exists
	_, err = storageapi.VerifyTokenRequest(token.Token).Send(ctx, client)
	assert.NoError(t, err)

	// Test rollback
	rb.Invoke(ctx)
	assert.Empty(t, d.DebugLogger().WarnMessages())
	_, err = storageapi.VerifyTokenRequest(token.Token).Send(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, "storage.tokenInvalid", err.(*storageapi.Error).ErrCode)
}
