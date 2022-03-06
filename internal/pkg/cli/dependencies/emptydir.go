package dependencies

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (v *container) EmptyDir() (filesystem.Fs, error) {
	if v.emptyDir == nil {
		// Project dir is not expected
		if v.LocalProjectExists() {
			return nil, ErrProjectDirFound
		}

		// Template dir is not expected
		if v.LocalTemplateExists() {
			return nil, ErrTemplateDirFound
		}

		// Repository dir is not expected
		if v.LocalTemplateRepositoryExists() {
			return nil, ErrRepositoryDirFound
		}

		// Read directory
		items, err := v.fs.ReadDir(`.`)
		if err != nil {
			return nil, err
		}

		// Filter out ignored files
		found := utils.NewMultiError()
		for _, item := range items {
			if !filesystem.IsIgnoredPath(item.Name(), item) {
				path := item.Name()
				if found.Len() > 5 {
					found.Append(fmt.Errorf(path + ` ...`))
					break
				} else {
					found.Append(fmt.Errorf(path))
				}
			}
		}

		// Directory must be empty
		if found.Len() > 0 {
			return nil, utils.PrefixError(fmt.Sprintf(`directory "%s" it not empty, found`, v.fs.BasePath()), found)
		}

		v.emptyDir = v.fs
	}

	return v.emptyDir, nil
}
