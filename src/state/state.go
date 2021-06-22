package state

import (
	"context"
	"go.uber.org/zap"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
)

func LoadState(manifest *model.Manifest, logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi) (*model.State, bool) {
	state := model.NewState(manifest.ProjectDir, manifest.Naming)

	logger.Debugf("Loading project remote state.")
	LoadRemoteState(state, ctx, api)

	logger.Debugf("Loading local state.")
	LoadLocalState(state, manifest, api)

	ok := state.LocalErrors().Len() == 0 && state.RemoteErrors().Len() == 0
	return state, ok
}
