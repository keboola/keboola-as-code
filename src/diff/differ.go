package diff

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/api"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

type Differ struct {
	ctx         context.Context
	api         *api.StorageApi
	logger      *zap.SugaredLogger
	stateLoaded bool
	state       *model.State
	results     []Result
	error       *utils.Error
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
	grp.Go(d.loadRemoteState(ctx))
	grp.Go(d.loadLocalState())
	err := grp.Wait()
	if err == nil {
		d.stateLoaded = true
	}
	return err
}

func (d *Differ) Diff() (*Results, error) {
	if !d.stateLoaded {
		panic("LoadState() must be called before Diff()")
	}

	// Diff all states
	d.results = []Result{}
	d.error = &utils.Error{}
	for _, b := range d.state.BranchesSlice() {
		d.diffOne(&BranchState{b})
	}
	for _, c := range d.state.ConfigsSlice() {
		d.diffOne(&ConfigState{c})
	}
	for _, r := range d.state.ConfigRowsSlice() {
		d.diffOne(&ConfigRowState{r})
	}

	// Check errors
	var err error
	if d.error.Len() > 0 {
		err = fmt.Errorf("%s", d.error)
	}

	return &Results{d.results}, err
}

func (d *Differ) diffOne(state ModelState) {
	result, err := state.diff()
	if err != nil {
		d.error.Add(err)
	} else {
		d.results = append(d.results, result)
	}
}

func (d *Differ) loadRemoteState(ctx context.Context) func() error {
	return func() error {
		d.logger.Debugf("Loading project remote state.")
		remoteErrors := d.api.LoadRemoteState(d.state, ctx)
		if remoteErrors.Len() > 0 {
			d.logger.Debugf("Project remote state load failed: %s", remoteErrors)
			return fmt.Errorf("cannot load project remote state: %s", remoteErrors)
		} else {
			d.logger.Debugf("Project remote state successfully loaded.")
		}
		return nil
	}
}

func (d *Differ) loadLocalState() func() error {
	return func() error {
		d.logger.Debugf("Loading project local state.")
		localErrors := model.LoadLocalState(d.state)
		if localErrors.Len() > 0 {
			d.logger.Debugf("Project local state load failed: %s", localErrors)
			return fmt.Errorf("cannot load project local state: %s", localErrors)
		} else {
			d.logger.Debugf("Project local state successfully loaded.")
		}
		return nil
	}
}
