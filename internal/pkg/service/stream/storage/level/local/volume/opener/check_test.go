package opener

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestCheckVolumeDir_Ok(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	err := checkVolumeDir(path)
	assert.NoError(t, err)
}

func TestCheckVolumeDir_NonExistentPath(t *testing.T) {
	t.Parallel()
	path := filesystem.Join("non-existent", "path")
	err := checkVolumeDir(path)
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrNotExist))
	}
}

func TestCheckVolumeDir_FileNotDir(t *testing.T) {
	t.Parallel()
	path := filesystem.Join(t.TempDir(), "file")

	// Create file
	assert.NoError(t, os.WriteFile(path, []byte("foo"), 0o640))

	err := checkVolumeDir(path)
	if assert.Error(t, err) {
		assert.Equal(t, fmt.Sprintf(`cannot open volume "%s": the path is not directory`, path), err.Error())
	}
}
