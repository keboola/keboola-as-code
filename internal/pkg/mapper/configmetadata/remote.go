package configmetadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *configMetadataMapper) AfterRemoteOperation(changes *model.RemoteChanges) error {
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

func (m *configMetadataMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	_, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	if recipe.ChangedFields.Has(`metadata`) {
		recipe.ChangedFields.Remove(`metadata`)
	}

	return nil
}
