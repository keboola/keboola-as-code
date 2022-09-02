package init

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	createMetaDir "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/metadir/create"
	createRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/create"
)

type dependencies interface {
	Logger() log.Logger
	EmptyDir() (filesystem.Fs, error)
}

func Run(ctx context.Context, d dependencies) (err error) {
	logger := d.Logger()

	// Empty dir
	emptyDir, err := d.EmptyDir()
	if err != nil {
		return err
	}

	// Create metadata dir
	if err := createMetaDir.Run(ctx, emptyDir, d); err != nil {
		return err
	}

	// Create manifest
	if _, err := createRepositoryManifest.Run(ctx, emptyDir, d); err != nil {
		return err
	}

	logger.Info("Repository init done.")
	return nil
}
