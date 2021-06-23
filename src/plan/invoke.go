package plan

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
)

func (p *Plan) Invoke(logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi, m *manifest.Manifest) error {
	errors := &utils.Error{}
	workers, _ := errgroup.WithContext(ctx)
	pool := api.NewPool()
	for _, action := range p.Actions {
		switch action.Type {
		case ActionSaveLocal:
			if err := SaveLocal(action.ObjectState, m, logger, workers); err != nil {
				errors.Add(err)
			}
		case ActionSaveRemote:
			if err := SaveRemote(api, action.Result, logger, pool); err != nil {
				errors.Add(err)
			}
		case ActionDeleteLocal:
			if err := DeleteLocal(action.ObjectState, m, logger, workers); err != nil {
				errors.Add(err)
			}
		case ActionDeleteRemote:
			if err := DeleteRemote(api, action.Result, logger, pool); err != nil {
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

	if err := local.DeleteEmptyDirectories(logger, m.ProjectDir); err != nil {
		errors.Add(err)
	}

	if errors.Len() > 0 {
		return fmt.Errorf("pull failed: %s", errors)
	}

	return nil
}
