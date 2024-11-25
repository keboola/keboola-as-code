package localfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalFs(t *testing.T) {
	t.Parallel()
	projectDir := t.TempDir()
	fs, err := New(projectDir)
	require.NoError(t, err)
	assert.Equal(t, projectDir, fs.BasePath())
}
