package store

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
)

func TestStore_GetTokenOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver1"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "export1"}

	time := iso8601.Time{}

	input := model.Token{
		Token:                 "test",
		ID:                    "test",
		Description:           "test",
		IsMaster:              false,
		CanManageBuckets:      false,
		CanManageTokens:       false,
		CanReadAllFileUploads: false,
		CanPurgeTrash:         false,
		Created:               time,
		Refreshed:             time,
		Expires:               nil,
		IsExpired:             false,
		IsDisabled:            false,
		Owner:                 storageapi.TokenOwner{},
		Admin:                 nil,
		Creator:               nil,
		BucketPermissions:     nil,
		ComponentAccess:       nil,
	}

	_, err := store.createTokenOp(ctx, exportKey, input).Do(ctx, store.client)
	assert.NoError(t, err)

	token, err := store.getTokenOp(ctx, exportKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, token.Value)

	etcdhelper.AssertKVs(t, store.client, `
<<<<<
secret/export/token/1000/receiver1/export1
-----
{
  "token": "test",
  "id": "test",
  "description": "test",
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
