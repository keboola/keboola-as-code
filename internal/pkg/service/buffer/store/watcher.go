package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type Watcher struct {
	mappings         sync.Map
	secrets          sync.Map
	slicesForExports sync.Map
	store            *Store
}

func NewWatcher(store *Store) (*Watcher, error) {
	w := &Watcher{
		mappings:         sync.Map{},
		secrets:          sync.Map{},
		slicesForExports: sync.Map{},
		store:            store,
	}
	return w, nil
}

func (w *Watcher) GetMappings(k key.ReceiverKey) (map[key.ExportKey]*model.Mapping, bool) {
	mappings, found := w.mappings.Load(k)
	if !found {
		return nil, false
	}
	return mappings.(map[key.ExportKey]*model.Mapping), true
}

func (w *Watcher) GetSecret(k key.ReceiverKey) (string, bool) {
	secret, found := w.secrets.Load(k)
	if !found {
		return "", false
	}
	return secret.(string), true
}

func (w *Watcher) GetSliceID(k key.ExportKey) (*time.Time, bool) {
	sliceID, found := w.slicesForExports.Load(k)
	if !found {
		return nil, false
	}
	sliceIDTyped := sliceID.(time.Time)
	return &sliceIDTyped, true
}

func (w *Watcher) Watch(ctx context.Context, logger log.Logger, client *etcd.Client) {
	handleErrors := func(err error) {
		logger.Error(err)
	}
	go func() {
		slicesCh := w.store.schema.Slices().Watch(ctx, client, handleErrors)
		mappingsCh := w.store.schema.Configs().Mappings().Watch(ctx, client, handleErrors)
		for {
			select {
			case <-ctx.Done():
				break
			case slice := <-slicesCh:
				w.handleSliceEvent(slice)
			case mapping := <-mappingsCh:
				w.handleMappingEvent(mapping)
			}
		}
	}()
}

func (w *Watcher) addExportMapping(recKey key.ReceiverKey, expKey key.ExportKey, mapping *model.Mapping) {
	mappings, _ := w.mappings.LoadOrStore(recKey, make(map[key.ExportKey]*model.Mapping))
	mappings.(map[key.ExportKey]*model.Mapping)[expKey] = mapping
}

func (w *Watcher) removeExportMapping(recKey key.ReceiverKey, expKey key.ExportKey) {
	mappings, _ := w.mappings.LoadOrStore(recKey, make(map[key.ExportKey]*model.Mapping))
	delete(mappings.(map[key.ExportKey]*model.Mapping), expKey)
}

// handleSliceEvent takes care of events on slice keys
// On Create add the slice ID to the export-slice map.
// On Update do nothing, don't care about slice content.
// On Delete remove the slice ID from the export-slice map.
func (w *Watcher) handleSliceEvent(event etcdop.EventT[model.Slice]) {
	keyParts := strings.Split(event.Key(), "/")
	if len(keyParts) != 7 {
		panic(fmt.Sprintf("invalid key in slice prefix: %s", event.Key()))
	}
	projectID, err := strconv.Atoi(keyParts[2])
	if err != nil {
		panic(fmt.Sprintf("invalid project ID in slice prefix: %s", event.Key()))
	}
	sliceID, err := time.Parse(key.TimeFormat, keyParts[6])
	if err != nil {
		panic(fmt.Sprintf("invalid slice ID in slice prefix: %s", event.Key()))
	}
	exportKey := key.ExportKey{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  projectID,
			ReceiverID: keyParts[3],
		},
		ExportID: keyParts[4],
	}
	switch event.Type {
	case etcdop.CreateEvent:
		w.slicesForExports.Store(exportKey, sliceID)
	case etcdop.DeleteEvent:
		resSliceID, found := w.slicesForExports.Load(exportKey)
		if found && sliceID == resSliceID {
			w.slicesForExports.Delete(exportKey)
		}
	}
}

// handleMappingEvent takes care of events on mapping keys
// On Create store the new mapping (a previous mapping revision will be rewritten).
// On Update do nothing (mappings are updated by adding new revisions).
// On Delete do nothing (mappings are updated by adding new revisions).
func (w *Watcher) handleMappingEvent(event etcdop.EventT[model.Mapping]) {
	keyParts := strings.Split(event.Key(), "/")
	if len(keyParts) != 7 {
		panic(fmt.Sprintf("invalid key in mapping prefix: %s", event.Key()))
	}
	projectID, err := strconv.Atoi(keyParts[3])
	if err != nil {
		panic(fmt.Sprintf("invalid project ID in mapping prefix: %s", event.Key()))
	}
	receiverKey := key.ReceiverKey{
		ProjectID:  projectID,
		ReceiverID: keyParts[4],
	}
	exportKey := key.ExportKey{
		ReceiverKey: receiverKey,
		ExportID:    keyParts[5],
	}
	switch event.Type {
	case etcdop.CreateEvent:
		w.addExportMapping(receiverKey, exportKey, &event.Value)
	}
}
