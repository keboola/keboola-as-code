package opener

import (
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func checkVolumeDir(path string) error {
	info, err := os.Stat(path)
	// Path must exist
	if err != nil {
		return errors.Errorf(`cannot open volume "%s": %w`, path, err)
	}

	// Path must be directory
	if !info.IsDir() {
		return errors.Errorf(`cannot open volume "%s": the path is not directory`, path)
	}

	return nil
}
