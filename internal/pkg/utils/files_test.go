package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileExists(t *testing.T) {
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

func TestCreateOrUpdateFile(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "file.txt")

	// Create empty file
	updated, err := CreateOrUpdateFile(path, []FileLine{})
	assert.False(t, updated)
	assert.NoError(t, err)
	assert.FileExists(t, path)
	assert.Equal(t, "", GetFileContent(path))

	// Add some lines
	updated, err = CreateOrUpdateFile(path, []FileLine{
		{Line: "foo"},
		{Line: "bar\n"},
		{Line: "BAZ1=123\n", Regexp: "^BAZ1="},
		{Line: "BAZ2=456\n", Regexp: "^BAZ2=.*$"},
	})
	assert.True(t, updated)
	assert.NoError(t, err)
	assert.FileExists(t, path)
	assert.Equal(t, "foo\nbar\nBAZ1=123\nBAZ2=456\n", GetFileContent(path))

	// Update some lines
	updated, err = CreateOrUpdateFile(path, []FileLine{
		{Line: "BAZ1=new123\n", Regexp: "^BAZ1="},
		{Line: "BAZ2=new456\n", Regexp: "^BAZ2=.*$"},
	})
	assert.True(t, updated)
	assert.NoError(t, err)
	assert.FileExists(t, path)
	assert.Equal(t, "foo\nbar\nBAZ1=new123\nBAZ2=new456\n", GetFileContent(path))
}
