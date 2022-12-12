package store

import (
	"context"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
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

func (w *Watcher) Watch(_ context.Context, _ etcd.Client) {
	// TODO
}

func (w *Watcher) addExportMapping(recKey key.ReceiverKey, expKey key.ExportKey, mapping *model.Mapping) {
	mappings, _ := w.mappings.LoadOrStore(recKey, make(map[key.ExportKey]*model.Mapping))
	mappings.(map[key.ExportKey]*model.Mapping)[expKey] = mapping
}

func (w *Watcher) removeExportMapping(recKey key.ReceiverKey, expKey key.ExportKey) {
	mappings, _ := w.mappings.LoadOrStore(recKey, make(map[key.ExportKey]*model.Mapping))
	delete(mappings.(map[key.ExportKey]*model.Mapping), expKey)
}
