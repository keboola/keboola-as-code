package store

import (
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
)

var receiver = model.Receiver{
	ReceiverBase: model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  100,
			ReceiverID: "r1",
		},
		Name:   "receiver 1",
		Secret: "secret1_jfkdsjklgflgkskfdjgklfdjlkjgfdlkgjflkrew",
	},
	Exports: []model.Export{
		{
			ExportBase: model.ExportBase{
				ExportKey: key.ExportKey{
					ReceiverKey: key.ReceiverKey{
						ProjectID:  100,
						ReceiverID: "r1",
					},
					ExportID: "e1",
				},
				Name:             "export 1",
				ImportConditions: model.DefaultConditions(),
			},
			Mapping: model.Mapping{
				MappingKey: key.MappingKey{
					ExportKey: key.ExportKey{
						ReceiverKey: key.ReceiverKey{
							ProjectID:  100,
							ReceiverID: "r1",
						},
						ExportID: "e1",
					},
					RevisionID: 1,
				},
				TableID: model.TableID{
					Stage:  "out",
					Bucket: "c-main",
					Table:  "import",
				},
				Incremental: true,
				Columns: []column.Column{
					column.Headers{Name: "headers"},
					column.Body{Name: "body"},
				},
			},
			Token: storageapi.Token{},
		},
	},
}

func TestStore_Watcher_GetMappings(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	w, err := NewWatcher(store)
	assert.NoError(t, err)
	expKey := receiver.Exports[0].ExportKey
	mapping := receiver.Exports[0].Mapping
	w.mappings.Store(receiver.ReceiverKey, map[key.ExportKey]*model.Mapping{expKey: &mapping})

	// Found
	exportsRes, found := w.GetMappings(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &mapping}, exportsRes)

	// Not found
	_, found = w.GetMappings(key.ReceiverKey{ProjectID: 101, ReceiverID: "r2"})
	assert.False(t, found)
}

func TestStore_Watcher_GetSecret(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	watcher, err := NewWatcher(store)
	assert.NoError(t, err)

	// Found
	watcher.secrets.Store(receiver.ReceiverKey, receiver.Secret)
	secRes, found := watcher.GetSecret(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, receiver.Secret, secRes)

	// Not found
	secRes, found = watcher.GetSecret(key.ReceiverKey{ProjectID: 123, ReceiverID: "r2"})
	assert.False(t, found)
	assert.Equal(t, "", secRes)
}

func TestStore_Watcher_GetSliceID(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	w, err := NewWatcher(store)
	assert.NoError(t, err)
	expKey := receiver.Exports[0].ExportKey
	sliceID, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	sliceID = sliceID.UTC()
	w.slicesForExports.Store(expKey, sliceID)

	// Found
	sRes, found := w.GetSliceID(expKey)
	assert.True(t, found)
	assert.Equal(t, sliceID, *sRes)

	// Not found
	_, found = w.GetSliceID(key.ExportKey{
		ReceiverKey: receiver.ReceiverKey,
		ExportID:    "e2",
	})
	assert.False(t, found)
}

func TestStore_Watcher_AddRemoveExportMapping(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	w, err := NewWatcher(store)
	assert.NoError(t, err)
	expKey := receiver.Exports[0].ExportKey
	mapping := receiver.Exports[0].Mapping

	// Add export mapping to new receiver
	w.addExportMapping(receiver.ReceiverKey, expKey, &mapping)
	res, found := w.mappings.Load(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &mapping}, res)

	// Add new export mapping to existing receiver
	newExpKey := key.ExportKey{
		ReceiverKey: receiver.ReceiverKey,
		ExportID:    "e2",
	}
	w.addExportMapping(receiver.ReceiverKey, newExpKey, &mapping)
	res, found = w.mappings.Load(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &mapping, newExpKey: &mapping}, res)

	// Add mapping to existing export
	newMapping := receiver.Exports[0].Mapping
	newMapping.Incremental = false
	w.addExportMapping(receiver.ReceiverKey, expKey, &newMapping)
	res, found = w.mappings.Load(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &newMapping, newExpKey: &mapping}, res)

	// Remove export mapping
	w.removeExportMapping(receiver.ReceiverKey, expKey)
	res, found = w.mappings.Load(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{newExpKey: &mapping}, res)
}
