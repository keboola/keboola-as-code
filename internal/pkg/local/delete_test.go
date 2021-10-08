package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func TestLocalDeleteModel(t *testing.T) {
	t.Parallel()
	manager := newTestLocalManager(t)
	fs := manager.fs

	record := &MockedRecord{}
	assert.NoError(t, manager.manifest.TrackRecord(record))
	_, found := manager.manifest.GetRecord(record.Key())
	assert.True(t, found)

	dirPath := record.Path()

	metaFilePath := manager.Naming().MetaFilePath(record.Path())
	metaFile := `{
  "myKey": "3",
  "Meta2": "4"
}
`
	configFilePath := manager.Naming().ConfigFilePath(record.Path())
	configFile := `{
  "foo": "bar"
}
`

	// Save files
	assert.NoError(t, fs.Mkdir(dirPath))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(metaFilePath, metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(configFilePath, configFile)))

	// Delete
	assert.NoError(t, manager.DeleteObject(record))

	// Assert
	_, found = manager.manifest.GetRecord(record.Key())
	assert.False(t, found)
	assert.False(t, fs.Exists(metaFilePath))
	assert.False(t, fs.Exists(configFilePath))
	assert.False(t, fs.Exists(dirPath))
}

func TestDeleteEmptyDirectories(t *testing.T) {
	t.Parallel()
	manager := newTestLocalManager(t)
	fs := manager.fs

	// Structure:
	// D .hidden
	// D .git
	//     D empty
	// D tracked-empty
	// D tracked-empty-sub
	//     D abc
	// D non-tracked-empty
	// D tracked
	//    F foo.txt
	// D non-tracked
	//    F foo.txt
	// D tracked-with-hidden
	//    D .git

	// Create structure
	assert.NoError(t, fs.Mkdir(`.hidden`))
	assert.NoError(t, fs.Mkdir(filesystem.Join(`.git`, `empty`)))
	assert.NoError(t, fs.Mkdir(`tracked-empty`))
	assert.NoError(t, fs.Mkdir(filesystem.Join(`tracked-empty-sub`, `abc`)))
	assert.NoError(t, fs.Mkdir(`non-tracked-empty`))
	assert.NoError(t, fs.Mkdir(`tracked`))
	assert.NoError(t, fs.Mkdir(`non-tracked`))
	assert.NoError(t, fs.Mkdir(filesystem.Join(`tracked-with-hidden`, `.git`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`tracked`, `foo.txt`), `bar`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`non-tracked`, `foo.txt`), `bar`)))

	// Delete
	trackedPaths := []string{
		`.hidden`,
		`tracked-empty`,
		`tracked-empty-sub`,
		`tracked`,
		`tracked-with-hidden`,
	}
	assert.NoError(t, manager.DeleteEmptyDirectories(trackedPaths))

	// Assert
	assert.False(t, fs.Exists(`tracked-empty`))
	assert.False(t, fs.Exists(`tracked-empty-sub`))

	assert.True(t, fs.Exists(`.hidden`))
	assert.True(t, fs.Exists(filesystem.Join(`.git`, `empty`)))
	assert.True(t, fs.Exists(`non-tracked-empty`))
	assert.True(t, fs.Exists(filesystem.Join(`tracked-with-hidden`, `.git`)))
	assert.True(t, fs.Exists(filesystem.Join(`tracked`, `foo.txt`)))
	assert.True(t, fs.Exists(filesystem.Join(`non-tracked`, `foo.txt`)))
}
