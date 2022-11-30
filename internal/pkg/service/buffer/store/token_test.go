package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_GetTokenOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver1"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "export1"}

	input := model.TokenForExport{ExportKey: exportKey, Token: model.Token{Token: "test"}}

	_, err := store.createTokenOp(ctx, input).Do(ctx, store.client)
	assert.NoError(t, err)

	token, err := store.getTokenOp(ctx, exportKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, token.Value)

	etcdhelper.AssertKVs(t, store.client, `
<<<<<
secret/export/token/1000/receiver1/export1
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export1",
  "token": "test",
  "id": "",
  "description": "",
  "isMasterToken": false,
  "canManageBuckets": false,
  "canManageTokens": false,
  "canReadAllFileUploads": false,
  "canPurgeTrash": false,
  "created": "0001-01-01T00:00:00Z",
  "refreshed": "0001-01-01T00:00:00Z",
  "expires": null,
  "isExpired": false,
  "isDisabled": false,
  "owner": {
    "id": 0,
    "name": "",
    "features": null
  }
}
>>>>>
`)
}

func TestStore_DeleteTokenOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver1"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "export1"}

	input := model.TokenForExport{ExportKey: exportKey, Token: model.Token{Token: "test"}}

	_, err := store.createTokenOp(ctx, input).Do(ctx, store.client)
	assert.NoError(t, err)

	token, err := store.getTokenOp(ctx, exportKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, token.Value)

	etcdhelper.AssertKVs(t, store.client, `
<<<<<
secret/export/token/1000/receiver1/export1
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export1",
  "token": "test",
  "id": "",
  "description": "",
  "isMasterToken": false,
  "canManageBuckets": false,
  "canManageTokens": false,
  "canReadAllFileUploads": false,
  "canPurgeTrash": false,
  "created": "0001-01-01T00:00:00Z",
  "refreshed": "0001-01-01T00:00:00Z",
  "expires": null,
  "isExpired": false,
  "isDisabled": false,
  "owner": {
    "id": 0,
    "name": "",
    "features": null
  }
}
>>>>>
`)

	_, err = store.deleteTokenOp(ctx, exportKey).Do(ctx, store.client)
	assert.NoError(t, err)

	etcdhelper.AssertKVs(t, store.client, ``)
}
