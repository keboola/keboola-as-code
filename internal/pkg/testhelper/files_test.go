//nolint:forbidigo
package testhelper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileExists(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "file.txt")

	// Create file
	file1, err := os.Create(filePath)
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	err = file1.Close()
	assert.NoError(t, err)

	// Assert
	assert.True(t, FileExists(filePath))
	assert.False(t, FileExists(tempDir+"/file-non-exists.txt"))
}

func TestGetFileContent(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "file.txt")

	// Create file
	file1, err := os.Create(filePath)
	assert.NoError(t, err)
	_, err = file1.WriteString("foo\n")
	assert.NoError(t, err)
	err = file1.Close()
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, "foo\n", GetFileContent(filePath))
}
