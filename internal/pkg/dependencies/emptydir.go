package dependencies

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

var (
	ErrProjectDirFound    = fmt.Errorf("project directory not expected, but found")
	ErrTemplateDirFound   = fmt.Errorf("template directory not expected, but found")
	ErrRepositoryDirFound = fmt.Errorf("repository directory not expected, but found")
)

func (c *common) EmptyDir() (filesystem.Fs, error) {
	if c.emptyDir == nil {
		// Get FS
		fs := c.Fs()

		// Project dir is not expected
		if c.LocalProjectExists() {
			return nil, ErrProjectDirFound
		}

		// Template dir is not expected
		if c.LocalTemplateExists() {
			return nil, ErrTemplateDirFound
		}

		// Repository dir is not expected
		if c.LocalTemplateRepositoryExists() {
			return nil, ErrRepositoryDirFound
		}

		// Read directory
		items, err := fs.ReadDir(`.`)
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
			return nil, utils.PrefixError(fmt.Sprintf(`directory "%s" it not empty, found`, fs.BasePath()), found)
		}

		c.emptyDir = fs
	}

	return c.emptyDir, nil
}
