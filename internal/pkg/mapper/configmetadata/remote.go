package configmetadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// configMetadataMapper add metadata to configurations loaded from API.
type configMetadataMapper struct {
	dependencies
	logger         log.Logger
	configMetadata map[model.BranchId]remote.ConfigMetadataResponse
}

type dependencies interface {
	StorageApi() (*remote.StorageApi, error)
}

func NewMapper(s *state.State, d dependencies) *configMetadataMapper {
	return &configMetadataMapper{dependencies: d, logger: s.Logger(), configMetadata: make(map[model.BranchId]remote.ConfigMetadataResponse)}
}

func (m *configMetadataMapper) OnRemoteChange(changes *model.RemoteChanges) error {
	// Process loaded objects
	errors := utils.NewMultiError()
	for _, objectState := range changes.Loaded() {
		if err := m.onRemoteLoad(objectState); err != nil {
			errors.Append(err)
		}
	}

	if errors.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(utils.PrefixError(`Warning`, errors))
	}
	return nil
}

func (m *configMetadataMapper) onRemoteLoad(objectState model.ObjectState) error {
	config, ok := objectState.RemoteState().(*model.Config)
	if !ok {
		return nil
	}
	metadata, found, err := m.GetMetadata(config.ConfigKey)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	config.Metadata = metadata
	return nil
}

func (m *configMetadataMapper) GetMetadata(configKey model.ConfigKey) (map[string]string, bool, error) {
	configMetadataForBranch, ok := m.configMetadata[configKey.BranchId]
	if !ok {
		res, err := m.GetMetadataFromApi(configKey.BranchId)
		if err != nil {
			return nil, false, err
		}
		configMetadataForBranch = *res
		m.configMetadata[configKey.BranchId] = configMetadataForBranch
	}
	for _, metadata := range configMetadataForBranch {
		if metadata.ComponentId == configKey.ComponentId && metadata.ConfigId == configKey.Id {
			result := make(map[string]string)
			for _, r := range metadata.Metadata {
				result[r.Key] = r.Value
			}
			return result, true, nil
		}
	}
	return nil, false, nil
}

func (m *configMetadataMapper) GetMetadataFromApi(branchId model.BranchId) (*remote.ConfigMetadataResponse, error) {
	api, err := m.StorageApi()
	if err != nil {
		return nil, err
	}
	return api.ListConfigMetadata(branchId)
}
