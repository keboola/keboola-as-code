package local_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/relatedpaths"
)

func TestUnitOfWork_Delete(t *testing.T) {
	t.Parallel()
	testMapperInst := &testMapper{}
	s, uow, fs, manifest := newTestUow(t, testMapperInst)

	// Fixtures
	key := fixtures.MockedKey{Id: "123"}
	object := fixtures.MockedObject{MockedKey: key}
	objectManifest := &fixtures.MockedManifest{MockedKey: key, PathValue: model.NewAbsPath("foo", "bar")}
	objectDir := objectManifest.Path()
	s.MustAdd(object)
	manifest.MustAdd(objectManifest)

	// Save files
	metaFilePath := filesystem.Join(objectDir.String(), `meta.json`)
	metaFile := `{"myKey": "3", "Meta2": "4"}`
	configFilePath := filesystem.Join(objectDir.String(), `config.json`)
	configFile := `{"foo": "bar"}`
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(metaFilePath, metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(configFilePath, configFile)))
	relatedPaths := relatedpaths.New(objectDir)
	relatedPaths.Add(metaFilePath, configFilePath)
	s.SetRelatedPaths(object.Key(), relatedPaths)

	// Delete
	uow.Delete(object.Key())
	assert.NoError(t, uow.Invoke())

	// Assert that object is deleted
	_, found := manifest.Get(object.Key())
	assert.False(t, found)
	assert.False(t, fs.Exists(metaFilePath))
	assert.False(t, fs.Exists(configFilePath))
	assert.True(t, fs.Exists(objectDir.String())) // all empty directories are deleted at the end of delete operation

	// Assert changes set
	assert.Equal(t, []string{"foo"}, testMapperInst.localChanges)
}
