package create

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type dependencies interface {
	Logger() *zap.SugaredLogger
	Fs() filesystem.Fs
}

func Run(d dependencies) error {
	if err := d.Fs().Mkdir(filesystem.MetadataDir); err != nil {
		return fmt.Errorf("cannot create metadata directory \"%s\": %w", filesystem.MetadataDir, err)
	}
	d.Logger().Infof("Created metadata directory \"%s\".", filesystem.MetadataDir)
	return nil
}
