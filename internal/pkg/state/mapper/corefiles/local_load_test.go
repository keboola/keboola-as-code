package corefiles_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestCoreFilesMapper_MapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := d.Fs()

	// Save files
	baseDir := model.NewAbsPath("foo", "bar")
	metaFile := `{"myKey": "3", "Meta2": "4"}`
	configFile := `{"foo": "bar"}`
	object := &fixtures.MockedObject{}
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(state.NamingGenerator().MetaFilePath(baseDir), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(state.NamingGenerator().ConfigFilePath(baseDir), configFile)))

	// Call mapper
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), baseDir, object)
	assert.NoError(t, state.Mapper().MapAfterLocalLoad(recipe))

	// Values are loaded and set
	assert.Equal(t, &fixtures.MockedObject{
		Foo1:  "",
		Foo2:  "",
		Meta1: "3",
		Meta2: "4",
		Config: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key:   "foo",
				Value: "bar",
			},
		}),
	}, object)
}

func TestCoreFilesMapper_MapAfterLocalLoad_SkipChildIfParentIsInvalid(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)

	// Init dir
	fs := d.Fs()
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	inputDir := filesystem.Join(testDir, `..`, `..`, `..`, `fixtures`, `local`, `branch-invalid-meta-json`)
	assert.NoError(t, aferofs.CopyFs2Fs(nil, inputDir, fs, ``))

	// Setup manifest
	state.Manifest().MustAdd(
		&model.BranchManifest{
			BranchKey: model.BranchKey{Id: 123},
			AbsPath:   model.NewAbsPath(``, `main`),
		},
		&model.ConfigManifestWithRows{
			ConfigManifest: model.ConfigManifest{
				ConfigKey: model.ConfigKey{BranchId: 123, ComponentId: `foo.bar`, Id: `456`},
				AbsPath:   model.NewAbsPath(`main`, `config`),
			},
		},
	)

	// Load all
	uow := state.NewUnitOfWork(context.Background(), model.NoFilter())
	uow.LoadAll()

	// Invoke and check error
	// Branch is invalid, so config does not read at all (no error: config does not exist).
	err := uow.Invoke()
	expectedErr := `
branch metadata file "main/meta.json" is invalid:
  - invalid character 'f' looking for beginning of object key string, offset: 3
`
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())

	// Check invalid and not found objects
	assert.Equal(t, []model.Key{model.BranchKey{Id: 123}}, state.InvalidObjects())
	assert.Equal(t, []model.Key{}, state.NotFoundObjects())
}
