package state

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
)

func LoadState(manifest *model.Manifest, logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi) (*model.State, error) {
	state := model.NewState(manifest.ProjectDir, manifest.Naming)
	if err := loadRemoteState(state, logger, ctx, api); err != nil {
		return nil, err
	}
	if err := loadLocalState(state, logger, api, manifest); err != nil {
		return nil, err
	}
	return state, nil
}

func loadRemoteState(target *model.State, logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi) error {
	logger.Debugf("Loading project remote state.")
	remoteErrors := LoadRemoteState(target, ctx, api)
	if remoteErrors.Len() > 0 {
		logger.Debugf("Project remote state load failed: %s", remoteErrors)
		return fmt.Errorf("cannot load project remote state: %s", remoteErrors)
	} else {
		logger.Debugf("Project remote state successfully loaded.")
	}
	return nil
}

func loadLocalState(target *model.State, logger *zap.SugaredLogger, api *remote.StorageApi, manifest *model.Manifest) error {
	logger.Debugf("Loading project local state.")
	localErrors := LoadLocalState(target, manifest, api)
	if localErrors.Len() > 0 {
		logger.Debugf("Project local state load failed: %s", localErrors)
		return fmt.Errorf("cannot load project local state: %s", localErrors)
	} else {
		logger.Debugf("Project local state successfully loaded.")
	}
	return nil
}
