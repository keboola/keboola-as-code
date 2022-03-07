package init

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	createEnvFiles "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/envfiles/create"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	createMetaDir "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/metadir/create"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	Pull            bool // run pull after init?
	ManifestOptions createManifest.Options
	Workflows       genWorkflows.Options
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
	EmptyDir() (filesystem.Fs, error)
}

func Run(o Options, d dependencies) (err error) {
	logger := d.Logger()

	fs, err := d.EmptyDir()
	if err != nil {
		return err
	}

	// Create metadata dir
	if err := createMetaDir.Run(fs, d); err != nil {
		return err
	}

	// Create manifest
	manifest, err := createManifest.Run(fs, o.ManifestOptions, d)
	if err != nil {
		return fmt.Errorf(`cannot create manifest: %w`, err)
	}

	// Create ENV files
	if err := createEnvFiles.Run(fs, d); err != nil {
		return err
	}

	// Related operations
	errors := utils.NewMultiError()

	// Generate CI workflows
	if err := genWorkflows.Run(fs, o.Workflows, d); err != nil {
		errors.Append(utils.PrefixError(`workflows generation failed`, err))
	}

	logger.Info("Init done.")

	// First pull
	if o.Pull {
		logger.Info()
		logger.Info(`Running pull.`)

		// Load project state
		prj := project.New(fs, manifest, d)
		projectState, err := prj.LoadState(loadState.InitOptions(o.Pull))
		if err != nil {
			return err
		}

		// Pull
		pullOptions := pull.Options{DryRun: false, LogUntrackedPaths: false}
		if err := pull.Run(projectState, pullOptions, d); err != nil {
			errors.Append(utils.PrefixError(`pull failed`, err))
		}
	}

	return errors.ErrorOrNil()
}
