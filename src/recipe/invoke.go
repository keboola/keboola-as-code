package recipe

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/local"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
)

func (r *Recipe) Invoke(ctx context.Context, manifest *model.Manifest, api *remote.StorageApi, logger *zap.SugaredLogger) error {
	errors := &utils.Error{}
	workers, _ := errgroup.WithContext(ctx)
	pool := api.NewPool()
	for _, action := range r.Actions {
		switch action.Type {
		case ActionSaveLocal:
			if err := local.SaveLocal(action.ObjectState, manifest, logger, workers); err != nil {
				errors.Add(err)
			}
		case ActionSaveRemote:
			if err := api.Save(action.Result, logger, pool); err != nil {
				errors.Add(err)
			}
		case ActionDeleteLocal:
			if err := local.DeleteLocal(action.ObjectState, manifest, logger, workers); err != nil {
				errors.Add(err)
			}
		case ActionDeleteRemote:
			if err := api.Delete(action.Result, logger, pool); err != nil {
				errors.Add(err)
			}
		default:
			panic(fmt.Errorf("unexpected action type"))
		}
	}

	if err := pool.StartAndWait(); err != nil {
		errors.Add(err)
	}

	if err := workers.Wait(); err != nil {
		errors.Add(err)
	}

	if errors.Len() > 0 {
		return fmt.Errorf("pull failed: %s", errors)
	}

	return nil
}
