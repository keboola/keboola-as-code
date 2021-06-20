package state

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
)

func LoadState(projectDir, metadataDir string, logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi) (*model.State, error) {
	state := model.NewState(projectDir)
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(loadRemoteState(state, logger, ctx, api))
	grp.Go(loadLocalState(state, logger, api, projectDir, metadataDir))
	err := grp.Wait()
	return state, err
}

func loadRemoteState(target *model.State, logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi) func() error {
	return func() error {
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
}

func loadLocalState(target *model.State, logger *zap.SugaredLogger, api *remote.StorageApi, projectDir, metadataDir string) func() error {
	return func() error {
		logger.Debugf("Loading project local state.")
		localErrors := LoadLocalState(target, api, projectDir, metadataDir)
		if localErrors.Len() > 0 {
			logger.Debugf("Project local state load failed: %s", localErrors)
			return fmt.Errorf("cannot load project local state: %s", localErrors)
		} else {
			logger.Debugf("Project local state successfully loaded.")
		}
		return nil
	}
}
