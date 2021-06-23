package diff

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/api"
	"keboola-as-code/src/model"
)

type Differ struct {
	ctx         context.Context
	api         *api.StorageApi
	logger      *zap.SugaredLogger
	stateLoaded bool
	state       *model.State
}

func NewDiffer(ctx context.Context, a *api.StorageApi, logger *zap.SugaredLogger, projectDir, metadataDir string) *Differ {
	d := &Differ{
		ctx:    ctx,
		api:    a,
		logger: logger,
		state:  model.NewState(projectDir, metadataDir),
	}
	return d
}

func (d *Differ) LoadState() error {
	grp, ctx := errgroup.WithContext(d.ctx)
	grp.Go(func() error {
		d.logger.Debugf("Loading project remote state.")
		remoteErrors := d.api.LoadRemoteState(d.state, ctx)
		if remoteErrors.Len() > 0 {
			d.logger.Debugf("Project remote state load failed: %s", remoteErrors)
			return fmt.Errorf("cannot load project remote state: %s", remoteErrors)
		} else {
			d.logger.Debugf("Project remote state successfully loaded.")
		}
		return nil
	})
	grp.Go(func() error {
		d.logger.Debugf("Loading project local state.")
		localErrors := model.LoadLocalState(d.state)
		if localErrors.Len() > 0 {
			d.logger.Debugf("Project local state load failed: %s", localErrors)
			return fmt.Errorf("cannot load project local state: %s", localErrors)
		} else {
			d.logger.Debugf("Project local state successfully loaded.")
		}
		return nil
	})
	err := grp.Wait()
	if err == nil {
		d.stateLoaded = true
	}
	return err
}

func (d *Differ) Diff() *Diff {
	if !d.stateLoaded {
		panic("LoadState() must be called before Diff()")
	}

	diff := &Diff{}
	return diff
}
