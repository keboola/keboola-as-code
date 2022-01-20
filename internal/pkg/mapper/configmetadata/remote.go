package configmetadata

import (
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// configMetadataMapper add metadata to configurations loaded from API.
type configMetadataMapper struct {
	dependencies
	logger log.Logger
	state  *state.State
}

type dependencies interface {
	StorageApi() (*remote.StorageApi, error)
}

func NewMapper(s *state.State, d dependencies) *configMetadataMapper {
	return &configMetadataMapper{dependencies: d, logger: s.Logger(), state: s}
}

func (m *configMetadataMapper) OnRemoteChange(changes *model.RemoteChanges) error {
	metadataMap, err := m.GetMetadataMap()
	if err != nil {
		return err
	}

	// Process loaded objects
	for _, objectState := range changes.Loaded() {
		m.onRemoteLoad(objectState, metadataMap)
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
		return
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
				metadataResponse := *response.Result().(*remote.ConfigMetadataResponse)
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
