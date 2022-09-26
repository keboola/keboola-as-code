// nolint: forbidigo
package localfs

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/basepathfs"
)

// New - LocalFs is abstraction of the local filesystem implemented by "os" package
// All paths are relative to the basePath.
func New(basePath string) (*basepathfs.BasePathFs, error) {
	if !filepath.IsAbs(basePath) {
		panic(fmt.Errorf(`base path "%s" must be absolute`, basePath))
	}
	return basepathfs.New(afero.NewOsFs(), basePath)
}
