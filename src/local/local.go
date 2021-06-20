package local

import (
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/model"
)

func SaveLocal(state model.ObjectState, manifest *model.Manifest, logger *zap.SugaredLogger, workers *errgroup.Group) error {
	workers.Go(func() error {
		return manifest.SaveModel(state.Manifest(), state.RemoteState(), logger)
	})
	return nil
}

func DeleteLocal(state model.ObjectState, manifest *model.Manifest, logger *zap.SugaredLogger, workers *errgroup.Group) error {
	workers.Go(func() error {
		return manifest.DeleteModel(state.Manifest(), state.LocalState(), logger)
	})
	return nil
}
