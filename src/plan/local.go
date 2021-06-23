package plan

import (
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/state"
)

func SaveLocal(state state.ObjectState, m *manifest.Manifest, logger *zap.SugaredLogger, workers *errgroup.Group) error {
	workers.Go(func() error {
		return local.SaveModel(logger, m, state.Manifest(), state.RemoteState())
	})
	return nil
}

func DeleteLocal(state state.ObjectState, m *manifest.Manifest, logger *zap.SugaredLogger, workers *errgroup.Group) error {
	workers.Go(func() error {
		return local.DeleteModel(logger, m, state.Manifest(), state.LocalState())
	})
	return nil
}
