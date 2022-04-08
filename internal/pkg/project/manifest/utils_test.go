package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

func TestProjectInfo(t *testing.T) {
	t.Parallel()

	fs := testfs.NewMemoryFs()

	_, err := ProjectInfo(fs, "my/manifest.json")
	assert.Error(t, err)
	assert.Equal(t, `manifest "my/manifest.json" not found`, err.Error())

	json := `{}`
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("my/manifest.json", json)))
	_, err = ProjectInfo(fs, "my/manifest.json")
	assert.Error(t, err)
	assert.Equal(t, `missing "project.id" key in "my/manifest.json"`, err.Error())

	json = `{"project": {"id": 123}}`
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("my/manifest.json", json)))
	_, err = ProjectInfo(fs, "my/manifest.json")
	assert.Error(t, err)
	assert.Equal(t, `missing "project.apiHost" key in "my/manifest.json"`, err.Error())

	json = `{"project": {"id": 123, "apiHost": "connection.keboola.com"}}`
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("my/manifest.json", json)))
	project, err := ProjectInfo(fs, "my/manifest.json")
	assert.NoError(t, err)
	assert.Equal(t, 123, project.Id)
	assert.Equal(t, "connection.keboola.com", project.ApiHost)
}
