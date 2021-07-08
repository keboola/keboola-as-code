package state

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"os"
	"path/filepath"
	"testing"
)

func TestPersist(t *testing.T) {

	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	assert.NoError(t, os.Mkdir(metadataDir, 0755))
	m, err := manifest.NewManifest(123, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	api, _ := remote.TestStorageApi(t)
	state := NewState(projectDir, api, m)
	assert.NotNil(t, state)
}
