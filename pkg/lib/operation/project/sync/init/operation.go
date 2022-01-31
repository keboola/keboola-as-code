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
	LocalProject() (*project.Project, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
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
	if err := createManifest.Run(fs, o.ManifestOptions, d); err != nil {
		return fmt.Errorf(`cannot create manifest: %w`, err)
	}

	// Create ENV files
	if err := createEnvFiles.Run(fs, d); err != nil {
		return err
	}

	// Related operations
	errors := utils.NewMultiError()

	// Generate CI workflows
	if err := genWorkflows.Run(o.Workflows, d); err != nil {
		errors.Append(utils.PrefixError(`workflows generation failed`, err))
	}

	logger.Info("Init done.")

	// First pull
	if o.Pull {
		logger.Info()
		logger.Info(`Running pull.`)
		pullOptions := pull.Options{
			DryRun:            false,
			Force:             false,
			LogUntrackedPaths: false,
		}
		if err := pull.Run(pullOptions, d); err != nil {
			errors.Append(utils.PrefixError(`pull failed`, err))
		}
	}

	return errors.ErrorOrNil()
}
