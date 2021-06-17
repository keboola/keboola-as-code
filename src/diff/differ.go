package diff

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/api"
	"keboola-as-code/src/model"
	"keboola-as-code/src/options"
	"keboola-as-code/src/utils"
)

type Differ struct {
	projectDir  string
	metadataDir string
	ctx         context.Context
	api         *api.StorageApi
	logger      *zap.SugaredLogger
	stateLoaded bool
	remoteState *model.State
	remoteErr   *utils.Error
	localState  *model.State
	localPaths  *model.PathsState
	localErr    *utils.Error
}

func NewDiffer(ctx context.Context, a *api.StorageApi, logger *zap.SugaredLogger, options *options.Options) *Differ {
	return &Differ{
		ctx:         ctx,
		api:         a,
		logger:      logger,
		projectDir:  options.ProjectDirectory(),
		metadataDir: options.MetadataDirectory(),
	}
}

func (d *Differ) LoadState() error {
	grp, ctx := errgroup.WithContext(d.ctx)
	grp.Go(func() error {
		d.logger.Debugf("Loading project remote state.")
		d.remoteState, d.remoteErr = d.api.LoadRemoteState(ctx)
		if d.remoteErr.Len() > 0 {
			d.logger.Debugf("Project remote state load failed: %s", d.remoteErr.Error())
			return fmt.Errorf("cannot load project remote state: %s", d.remoteErr.Error())
		} else {
			d.logger.Debugf("Project remote state successfully loaded.")
		}
		return nil
	})
	grp.Go(func() error {
		d.logger.Debugf("Loading project local state.")
		d.localState, d.localPaths, d.localErr = model.LoadLocalState(d.projectDir, d.metadataDir)
		if d.localErr.Len() > 0 {
			d.logger.Debugf("Project local state load failed: %s", d.remoteErr.Error())
			return fmt.Errorf("cannot load project local state: %s", d.remoteErr.Error())
		} else {
			d.logger.Debugf("Project local state successfully loaded.")
		}
		return nil
	})
	err := grp.Wait()
	if err != nil {
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
