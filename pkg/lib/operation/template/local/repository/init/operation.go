package init

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	createMetaDir "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/metadir/create"
)

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	EmptyDir() (filesystem.Fs, error)
	CreateTemplateRepositoryManifest() (*manifest.Manifest, error)
}

func Run(d dependencies) (err error) {
	logger := d.Logger()

	// Create metadata dir
	if err := createMetaDir.Run(d); err != nil {
		return err
	}

	// Create manifest
	if _, err := d.CreateTemplateRepositoryManifest(); err != nil {
		return err
	}

	logger.Info("Repository init done.")
	return nil
}
