package create

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	EmptyDir() (filesystem.Fs, error)
}

func Run(d dependencies) error {
	fs, err := d.EmptyDir()
	if err != nil {
		return err
	}

	if err := fs.Mkdir(filesystem.MetadataDir); err != nil {
		return fmt.Errorf("cannot create metadata directory \"%s\": %w", filesystem.MetadataDir, err)
	}

	d.Logger().Infof("Created metadata directory \"%s\".", filesystem.MetadataDir)
	return nil
}
