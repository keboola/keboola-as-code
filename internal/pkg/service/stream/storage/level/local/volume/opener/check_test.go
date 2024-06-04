package opener

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestCheckVolumeDir_Ok(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	err := checkVolumeDir(path)
	require.NoError(t, err)
}

func TestCheckVolumeDir_NonExistentPath(t *testing.T) {
	t.Parallel()
	path := filepath.Join("non-existent", "path")
	err := checkVolumeDir(path)
	if assert.Error(t, err) {
		assert.True(t, errors.Is(err, os.ErrNotExist))
	}
}

func TestCheckVolumeDir_FileNotDir(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "file")

	// Create file
	require.NoError(t, os.WriteFile(path, []byte("foo"), 0o640))

	err := checkVolumeDir(path)
	if assert.Error(t, err) {
		assert.Equal(t, fmt.Sprintf(`cannot open volume "%s": the path is not directory`, path), err.Error())
	}
}
