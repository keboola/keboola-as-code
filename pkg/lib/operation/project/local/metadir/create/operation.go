package create

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(fs filesystem.Fs, d dependencies) error {
	if err := fs.Mkdir(filesystem.MetadataDir); err != nil {
		return fmt.Errorf("cannot create metadata directory \"%s\": %w", filesystem.MetadataDir, err)
	}

	d.Logger().Infof("Created metadata directory \"%s\".", filesystem.MetadataDir)
	return nil
}
