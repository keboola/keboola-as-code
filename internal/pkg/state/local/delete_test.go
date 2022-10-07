package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
)

func TestLocalDeleteModel(t *testing.T) {
	t.Parallel()
	manager := newTestLocalManager(t, nil)
	fs := manager.fs

	record := &fixtures.MockedManifest{}
	assert.NoError(t, manager.manifest.PersistRecord(record))
	_, found := manager.manifest.GetRecord(record.Key())
	assert.True(t, found)

	dirPath := record.Path()

	metaFilePath := manager.NamingGenerator().MetaFilePath(record.Path())
	metaFile := `{
  "myKey": "3",
  "Meta2": "4"
}
`
	configFilePath := manager.NamingGenerator().ConfigFilePath(record.Path())
	configFile := `{
  "foo": "bar"
}
`

	// Save files
	assert.NoError(t, fs.Mkdir(dirPath))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(metaFilePath, metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(configFilePath, configFile)))
	record.AddRelatedPath(metaFilePath)
	record.AddRelatedPath(configFilePath)

	// Delete
	assert.NoError(t, manager.deleteObject(record))

	// Assert
	_, found = manager.manifest.GetRecord(record.Key())
	assert.False(t, found)
	assert.False(t, fs.Exists(metaFilePath))
	assert.False(t, fs.Exists(configFilePath))
	assert.True(t, fs.Exists(dirPath)) // all empty directories are deleted at the end of delete operation
}

func TestDeleteEmptyDirectories(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()

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
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`tracked`, `foo.txt`), `bar`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`non-tracked`, `foo.txt`), `bar`)))

	// Delete
	trackedPaths := []string{
		`.hidden`,
		`tracked-empty`,
		`tracked-empty-sub`,
		`tracked`,
		`tracked-with-hidden`,
	}
	assert.NoError(t, DeleteEmptyDirectories(fs, trackedPaths))

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
