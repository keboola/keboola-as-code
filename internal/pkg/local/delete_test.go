package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/components"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLocalDeleteModel(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))

	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(1, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	manager := NewManager(logger, m, components.NewProvider(nil))

	metaFile := `{
  "myKey": "3",
  "Meta2": "4"
}
`
	configFile := `{
  "foo": "bar"
}
`
	record := &MockedRecord{}
	assert.NoError(t, m.TrackRecord(record))
	_, found := m.GetRecord(record.Key())
	assert.True(t, found)

	// Save files
	dirAbs := filepath.Join(projectDir, record.RelativePath())
	metaFileAbs := filepath.Join(projectDir, manager.Naming().MetaFilePath(record.RelativePath()))
	configFileAbs := filepath.Join(projectDir, manager.Naming().ConfigFilePath(record.RelativePath()))
	assert.NoError(t, os.MkdirAll(dirAbs, 0750))
	assert.NoError(t, os.WriteFile(metaFileAbs, []byte(metaFile), 0640))
	assert.NoError(t, os.WriteFile(configFileAbs, []byte(configFile), 0640))

	// Delete
	assert.NoError(t, manager.DeleteModel(record))

	// Assert
	_, found = m.GetRecord(record.Key())
	assert.False(t, found)
	assert.NoFileExists(t, metaFileAbs)
	assert.NoFileExists(t, configFileAbs)
	assert.NoFileExists(t, dirAbs)
}

func TestDeleteEmptyDirectories(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(1, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	manager := NewManager(logger, m, components.NewProvider(nil))

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
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `.hidden`), 0755))
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `.git`, `empty`), 0755))
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `tracked-empty`), 0755))
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `tracked-empty-sub`, `abc`), 0755))
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `non-tracked-empty`), 0755))
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `tracked`), 0755))
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `non-tracked`), 0755))
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, `tracked-with-hidden`, `.git`), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, `tracked`, `foo.txt`), []byte(`bar`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, `non-tracked`, `foo.txt`), []byte(`bar`), 0644))

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
	assert.NoDirExists(t, filepath.Join(projectDir, `tracked-empty`))
	assert.NoDirExists(t, filepath.Join(projectDir, `tracked-empty-sub`))

	assert.DirExists(t, filepath.Join(projectDir, `.hidden`))
	assert.DirExists(t, filepath.Join(projectDir, `.git`, `empty`))
	assert.DirExists(t, filepath.Join(projectDir, `non-tracked-empty`))
	assert.DirExists(t, filepath.Join(projectDir, `tracked-with-hidden`, `.git`))
	assert.FileExists(t, filepath.Join(projectDir, `tracked`, `foo.txt`))
	assert.FileExists(t, filepath.Join(projectDir, `non-tracked`, `foo.txt`))
}
