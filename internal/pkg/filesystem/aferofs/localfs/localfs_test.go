package localfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLocalFs(t *testing.T) {
	t.Parallel()
	projectDir := t.TempDir()
	fs, err := New(projectDir)
	assert.NoError(t, err)
	assert.Equal(t, projectDir, fs.BasePath())
}
