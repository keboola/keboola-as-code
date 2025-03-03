package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
)

func TestLocalDeleteModel(t *testing.T) {
	t.Parallel()
	manager := newTestLocalManager(t, nil)
	fs := manager.fs
	ctx := t.Context()

	record := &fixtures.MockedManifest{}
	require.NoError(t, manager.manifest.PersistRecord(record))
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
	require.NoError(t, fs.Mkdir(ctx, dirPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(metaFilePath, metaFile)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configFilePath, configFile)))
	record.AddRelatedPath(metaFilePath)
	record.AddRelatedPath(configFilePath)

	// Delete
	require.NoError(t, manager.deleteObject(ctx, record))

	// Assert
	_, found = manager.manifest.GetRecord(record.Key())
	assert.False(t, found)
	assert.False(t, fs.Exists(ctx, metaFilePath))
	assert.False(t, fs.Exists(ctx, configFilePath))
	assert.True(t, fs.Exists(ctx, dirPath)) // all empty directories are deleted at the end of delete operation
}

func TestDeleteEmptyDirectories(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	ctx := t.Context()

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
	require.NoError(t, fs.Mkdir(ctx, `.hidden`))
	require.NoError(t, fs.Mkdir(ctx, filesystem.Join(`.git`, `empty`)))
	require.NoError(t, fs.Mkdir(ctx, `tracked-empty`))
	require.NoError(t, fs.Mkdir(ctx, filesystem.Join(`tracked-empty-sub`, `abc`)))
	require.NoError(t, fs.Mkdir(ctx, `non-tracked-empty`))
	require.NoError(t, fs.Mkdir(ctx, `tracked`))
	require.NoError(t, fs.Mkdir(ctx, `non-tracked`))
	require.NoError(t, fs.Mkdir(ctx, filesystem.Join(`tracked-with-hidden`, `.git`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`tracked`, `foo.txt`), `bar`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`non-tracked`, `foo.txt`), `bar`)))

	// Delete
	trackedPaths := []string{
		`.hidden`,
		`tracked-empty`,
		`tracked-empty-sub`,
		`tracked`,
		`tracked-with-hidden`,
	}
	require.NoError(t, DeleteEmptyDirectories(ctx, fs, trackedPaths))

	// Assert
	assert.False(t, fs.Exists(ctx, `tracked-empty`))
	assert.False(t, fs.Exists(ctx, `tracked-empty-sub`))

	assert.True(t, fs.Exists(ctx, `.hidden`))
	assert.True(t, fs.Exists(ctx, filesystem.Join(`.git`, `empty`)))
	assert.True(t, fs.Exists(ctx, `non-tracked-empty`))
	assert.True(t, fs.Exists(ctx, filesystem.Join(`tracked-with-hidden`, `.git`)))
	assert.True(t, fs.Exists(ctx, filesystem.Join(`tracked`, `foo.txt`)))
	assert.True(t, fs.Exists(ctx, filesystem.Join(`non-tracked`, `foo.txt`)))
}
