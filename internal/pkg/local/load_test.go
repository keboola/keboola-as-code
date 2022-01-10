package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestLocalLoadModel(t *testing.T) {
	t.Parallel()
	manager, _ := newTestLocalManager(t)
	fs := manager.fs

	metaFile := `{
  "myKey": "3",
  "Meta2": "4"
}
`
	configFile := `{
  "foo": "bar"
}
`
	// Save files
	target := &fixtures.MockedObject{}
	record := &fixtures.MockedManifest{}
	assert.NoError(t, fs.Mkdir(record.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(manager.NamingGenerator().MetaFilePath(record.Path()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(manager.NamingGenerator().ConfigFilePath(record.Path()), configFile)))

	// Load
	found, err := manager.loadObject(record, target)
	assert.True(t, found)
	assert.NoError(t, err)

	// Assert
	config := orderedmap.New()
	config.Set("foo", "bar")
	assert.Equal(t, &fixtures.MockedObject{
		Foo1:   "",
		Foo2:   "",
		Meta1:  "3",
		Meta2:  "4",
		Config: config,
	}, target)
}

func TestLocalLoadModelNotFound(t *testing.T) {
	t.Parallel()
	manager, _ := newTestLocalManager(t)

	// Save files
	target := &fixtures.MockedObject{}
	record := &fixtures.MockedManifest{}

	// Load
	found, err := manager.loadObject(record, target)
	assert.False(t, found)
	assert.Error(t, err)
	assert.Equal(t, "kind \"test\" not found", err.Error())
}
