package configmetadata

import (
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *configMetadataMapper) AfterRemoteOperation(changes *model.RemoteChanges) error {
	metadataMap, err := m.GetMetadataMap()
	if err != nil {
		return err
	}

	// Process loaded objects
	for _, objectState := range changes.Loaded() {
		m.onRemoteLoad(objectState, metadataMap)
	}

	// Process saved objects
	if len(changes.Saved()) > 0 {
		api, err := m.StorageApi()
		if err != nil {
			return err
		}
		pool := api.NewPool()
		for _, objectState := range changes.Saved() {
			config, ok := objectState.RemoteState().(*model.Config)
			if !ok {
				continue
			}
			if len(config.Metadata) > 0 {
				pool.Request(api.UpdateConfigMetadataRequest(config)).Send()
			}
		}
		err = pool.StartAndWait()
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *configMetadataMapper) onRemoteLoad(objectState model.ObjectState, metadataMap map[string]map[string]string) {
	config, ok := objectState.RemoteState().(*model.Config)
	if !ok {
		return
	}
	metadata, found := metadataMap[config.ConfigKey.String()]
	if !found {
		metadata = make(map[string]string)
	}
	config.Metadata = metadata
}

// GetMetadataMap - Get metadata for each branch from the API and transform the response to an optimized map.
func (m *configMetadataMapper) GetMetadataMap() (map[string]map[string]string, error) {
	api, err := m.StorageApi()
	if err != nil {
		return nil, err
	}
	pool := api.NewPool()
	lock := &sync.Mutex{}
	configMetadata := make(map[string]map[string]string)
	for _, b := range m.state.Branches() {
		pool.
			Request(api.ListConfigMetadataRequest(b.Id)).
			OnSuccess(func(response *client.Response) {
				lock.Lock()
				defer lock.Unlock()
				metadataResponse := *response.Result().(*storageapi.ConfigMetadataResponse)
				for key, metadata := range metadataResponse.MetadataMap(b.Id) {
					metadataMap := make(map[string]string)
					for _, m := range metadata {
						metadataMap[m.Key] = m.Value
					}
					configMetadata[key.String()] = metadataMap
				}
			}).
			Send()
	}
	err = pool.StartAndWait()
	if err != nil {
		return nil, err
	}
	return configMetadata, nil
}
