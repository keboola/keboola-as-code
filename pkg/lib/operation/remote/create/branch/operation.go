package branch

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/save"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/sync/pull"
)

type Options struct {
	Name string
	Pull bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
	LoadStateOnce(loadOptions loadState.Options) (*state.State, error)
}

func Run(o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// Get manifest
	projectManifest, err := d.ProjectManifest()
	if err != nil {
		return err
	}

	// Create branch by API
	branch := &model.Branch{Name: o.Name}
	if _, err := storageApi.CreateBranch(branch); err != nil {
		return fmt.Errorf(`cannot create branch: %w`, err)
	}

	// Add new branch to the allowed branches if needed
	if projectManifest.IsObjectIgnored(branch) {
		allowedBranches := projectManifest.AllowedBranches()
		allowedBranches = append(allowedBranches, model.AllowedBranch(branch.Id.String()))
		projectManifest.SetAllowedBranches(allowedBranches)

		// Save manifest
		if _, err := saveManifest.Run(d); err != nil {
			return err
		}
	}

	logger.Info(fmt.Sprintf(`Created new %s "%s".`, branch.Kind().Name, branch.Name))

	// Pull
	if o.Pull {
		logger.Info()
		logger.Info(`Pulling objects to the local directory.`)
		pullOptions := pull.Options{DryRun: false, LogUntrackedPaths: false}
		if err := pull.Run(pullOptions, d); err != nil {
			return utils.PrefixError(`pull failed`, err)
		}
	}

	return nil
}
