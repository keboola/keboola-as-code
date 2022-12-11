package store

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
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
	w := NewWatcher(store)
	expKey := receiver.Exports[0].ExportKey
	mapping := receiver.Exports[0].Mapping
	w.mappings[receiver.ReceiverKey] = map[key.ExportKey]*model.Mapping{expKey: &mapping}

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
	w := NewWatcher(store)

	// Found
	w.secrets.Store(receiver.ReceiverKey, receiver.Secret)
	secRes, found := w.GetSecret(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, receiver.Secret, secRes)

	// Not found
	secRes, found = w.GetSecret(key.ReceiverKey{ProjectID: 123, ReceiverID: "r2"})
	assert.False(t, found)
	assert.Equal(t, "", secRes)
}

func TestStore_Watcher_GetSliceID(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	w := NewWatcher(store)
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
	w := NewWatcher(store)
	expKey := receiver.Exports[0].ExportKey
	mapping := receiver.Exports[0].Mapping

	// Add export mapping to new receiver
	w.addExportMapping(receiver.ReceiverKey, expKey, &mapping)
	res, found := w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &mapping}, res)

	// Add new export mapping to existing receiver
	newExpKey := key.ExportKey{
		ReceiverKey: receiver.ReceiverKey,
		ExportID:    "e2",
	}
	w.addExportMapping(receiver.ReceiverKey, newExpKey, &mapping)
	res, found = w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &mapping, newExpKey: &mapping}, res)

	// Add mapping to existing export
	newMapping := receiver.Exports[0].Mapping
	newMapping.Incremental = false
	w.addExportMapping(receiver.ReceiverKey, expKey, &newMapping)
	res, found = w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &newMapping, newExpKey: &mapping}, res)

	// Remove export mapping
	w.removeExportMapping(receiver.ReceiverKey, expKey)
	res, found = w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{newExpKey: &mapping}, res)
}

func TestStore_Watcher_HandleSliceEvent(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	now, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	now = now.UTC()
	store.clock.(*clock.Mock).Set(now)
	w := NewWatcher(store)
	expKey := receiver.Exports[0].ExportKey

	// Create new slice - add value to slicesForExports for the export key.
	sliceID1, _ := time.Parse(key.TimeFormat, "2006-01-02T15:04:05.000Z")
	w.handleSliceEvent(etcdop.EventT[model.Slice]{
		Type: etcdop.CreateEvent,
		Kv: &op.KeyValue{
			Key: []byte("slice/100/r1/e1/2006-01-01T15:04:05.000Z/" + key.FormatTime(sliceID1)),
		},
		Value: model.Slice{SliceNumber: 1},
	})
	val, found := w.slicesForExports.Load(expKey)
	assert.True(t, found)
	assert.Equal(t, sliceID1, val)

	// Create new slice for the same export - replace value in slicesForExports for the export key.
	sliceID2, _ := time.Parse(key.TimeFormat, "2006-01-03T15:04:05.000Z")
	w.handleSliceEvent(etcdop.EventT[model.Slice]{
		Type: etcdop.CreateEvent,
		Kv: &op.KeyValue{
			Key: []byte("slice/100/r1/e1/2006-01-01T15:04:05.000Z/" + key.FormatTime(sliceID2)),
		},
		Value: model.Slice{},
	})
	val, found = w.slicesForExports.Load(expKey)
	assert.True(t, found)
	assert.Equal(t, sliceID2, val)

	// Delete the original slice - Keep the value in slicesForExports for the export key pointing to the new slice.
	w.handleSliceEvent(etcdop.EventT[model.Slice]{
		Type: etcdop.DeleteEvent,
		Kv: &op.KeyValue{
			Key: []byte("slice/100/r1/e1/2006-01-01T15:04:05.000Z/" + key.FormatTime(sliceID1)),
		},
		Value: model.Slice{},
	})
	_, found = w.slicesForExports.Load(expKey)
	assert.True(t, found)

	// Delete the new slice - Remove the value in slicesForExports for the export key.
	w.handleSliceEvent(etcdop.EventT[model.Slice]{
		Type: etcdop.DeleteEvent,
		Kv: &op.KeyValue{
			Key: []byte("slice/100/r1/e1/2006-01-01T15:04:05.000Z/" + key.FormatTime(sliceID2)),
		},
		Value: model.Slice{},
	})
	_, found = w.slicesForExports.Load(expKey)
	assert.False(t, found)
}

func TestStore_Watcher_HandleMappingEvent(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	now, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	now = now.UTC()
	store.clock.(*clock.Mock).Set(now)
	w := NewWatcher(store)
	expKey := receiver.Exports[0].ExportKey
	mapping := receiver.Exports[0].Mapping

	// Create mapping
	w.handleMappingEvent(etcdop.EventT[model.Mapping]{
		Type: etcdop.CreateEvent,
		Kv: &op.KeyValue{
			Key: []byte("config/mapping/revision/100/r1/e1/1"),
		},
		Value: mapping,
	})
	mappings, found := w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &mapping}, mappings)

	// Create mapping for the same export - replace mapping in w.mappings
	newMapping := receiver.Exports[0].Mapping
	newMapping.Incremental = false
	w.handleMappingEvent(etcdop.EventT[model.Mapping]{
		Type: etcdop.CreateEvent,
		Kv: &op.KeyValue{
			Key: []byte("config/mapping/revision/100/r1/e1/2"),
		},
		Value: newMapping,
	})
	mappings, found = w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &newMapping}, mappings)

	// Create mapping for another export
	newExpKey := key.ExportKey{
		ReceiverKey: receiver.ReceiverKey,
		ExportID:    "e2",
	}
	w.handleMappingEvent(etcdop.EventT[model.Mapping]{
		Type: etcdop.CreateEvent,
		Kv: &op.KeyValue{
			Key: []byte("config/mapping/revision/100/r1/e2/1"),
		},
		Value: mapping,
	})
	mappings, found = w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey: &newMapping, newExpKey: &mapping}, mappings)
}

func TestStore_Watcher_HandleExportEvent(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	now, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	now = now.UTC()
	store.clock.(*clock.Mock).Set(now)
	w := NewWatcher(store)
	expKey := receiver.Exports[0].ExportKey
	mapping := receiver.Exports[0].Mapping

	// Init watcher store
	expKey2 := key.ExportKey{ExportID: "e2", ReceiverKey: receiver.ReceiverKey}
	mapping2 := receiver.Exports[0].Mapping
	sliceID, _ := time.Parse(key.TimeFormat, "2006-01-03T15:04:05.000Z")
	w.slicesForExports.Store(expKey, sliceID)
	w.mappings[receiver.ReceiverKey] = map[key.ExportKey]*model.Mapping{expKey: &mapping, expKey2: &mapping2}

	// Delete export
	w.handleExportEvent(etcdop.EventT[model.ExportBase]{
		Type: etcdop.DeleteEvent,
		Kv: &op.KeyValue{
			Key: []byte("config/export/100/r1/e1"),
		},
	})
	_, found := w.slicesForExports.Load(expKey)
	assert.False(t, found)
	mappings, found := w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{expKey2: &mapping2}, mappings)
}

func TestStore_Watcher_HandleReceiverEvent(t *testing.T) {
	t.Parallel()

	// Init watcher
	store := newStoreForTest(t)
	now, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	now = now.UTC()
	store.clock.(*clock.Mock).Set(now)
	w := NewWatcher(store)

	// Create receiver
	w.handleReceiverEvent(etcdop.EventT[model.ReceiverBase]{
		Type: etcdop.CreateEvent,
		Kv: &op.KeyValue{
			Key: []byte("config/receiver/100/r1"),
		},
		Value: model.ReceiverBase{Secret: "sec1"},
	})
	secret, found := w.secrets.Load(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, "sec1", secret)

	// Update receiver
	w.handleReceiverEvent(etcdop.EventT[model.ReceiverBase]{
		Type: etcdop.UpdateEvent,
		Kv: &op.KeyValue{
			Key: []byte("config/receiver/100/r1"),
		},
		Value: model.ReceiverBase{Secret: "sec2"},
	})
	secret, found = w.secrets.Load(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, "sec2", secret)

	// Delete receiver
	w.handleReceiverEvent(etcdop.EventT[model.ReceiverBase]{
		Type: etcdop.DeleteEvent,
		Kv: &op.KeyValue{
			Key: []byte("config/receiver/100/r1"),
		},
	})
	_, found = w.secrets.Load(receiver.ReceiverKey)
	assert.False(t, found)
}

func TestStore_Watcher_Watch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	export := receiver.Exports[0]

	// Create receiver
	_, err := store.createReceiverBaseOp(ctx, receiver.ReceiverBase).Do(ctx, store.client)
	assert.NoError(t, err)
	// Create export
	_, err = store.createExportBaseOp(ctx, export.ExportBase).Do(ctx, store.client)
	assert.NoError(t, err)
	// Create mapping
	_, err = store.createMappingOp(ctx, export.Mapping).Do(ctx, store.client)
	assert.NoError(t, err)

	// Init watcher
	w := NewWatcher(store)
	w.Watch(ctx, log.NewNopLogger(), store.client)
	time.Sleep(2 * time.Second)

	// Check that the receiver, export and mapping events were created from the existing data
	secret, found := w.secrets.Load(receiver.ReceiverKey)
	assert.True(t, found)
	assert.Equal(t, receiver.Secret, secret)
	mappings, found := w.mappings[export.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{export.ExportKey: &export.Mapping}, mappings)

	// Check slice watcher - add slice
	fileID, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	fileID = fileID.UTC()
	sliceID := fileID.Add(time.Hour)
	slice := model.Slice{
		SliceKey: key.SliceKey{
			FileKey: key.FileKey{
				ExportKey: export.ExportKey,
				FileID:    fileID,
			},
			SliceID: sliceID,
		},
		SliceNumber: 1,
	}
	_, found = w.slicesForExports.Load(export.ExportKey)
	assert.False(t, found)
	_, err = store.createSliceOp(ctx, slice).Do(ctx, store.client)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	res, found := w.slicesForExports.Load(export.ExportKey)
	assert.True(t, found)
	assert.Equal(t, sliceID, res)

	// Check mapping watcher - add mapping
	newRecKey := key.ReceiverKey{ProjectID: 100, ReceiverID: "r2"}
	newExpKey := key.ExportKey{ReceiverKey: newRecKey, ExportID: "e2"}
	newMapping := export.Mapping
	newMapping.MappingKey = key.MappingKey{ExportKey: newExpKey, RevisionID: 1}
	_, err = store.createMappingOp(ctx, newMapping).Do(ctx, store.client)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	mappings, found = w.mappings[newRecKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{newExpKey: &newMapping}, mappings)

	// Check export watcher - delete export
	_, err = store.deleteExportBaseOp(ctx, export.ExportKey).Do(ctx, store.client)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	_, found = w.slicesForExports.Load(export.ExportKey)
	assert.False(t, found)
	mappings, found = w.mappings[receiver.ReceiverKey]
	assert.True(t, found)
	assert.Equal(t, map[key.ExportKey]*model.Mapping{}, mappings)

	// Check receiver watcher - delete receiver
	_, found = w.secrets.Load(receiver.ReceiverKey)
	assert.True(t, found)
	_, err = store.deleteReceiverBaseOp(ctx, receiver.ReceiverKey).Do(ctx, store.client)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	_, found = w.secrets.Load(receiver.ReceiverKey)
	assert.False(t, found)
}
