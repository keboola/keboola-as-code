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
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestLoadCoreFiles(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := d.Fs()

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
	object := &fixtures.MockedObject{}
	manifest := &fixtures.MockedManifest{}
	assert.NoError(t, fs.Mkdir(manifest.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(state.NamingGenerator().MetaFilePath(manifest.Path()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(state.NamingGenerator().ConfigFilePath(manifest.Path()), configFile)))

	// Call mapper
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), manifest, object)
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

func TestLoadCoreFiles_SkipChildrenLoadIfParentIsInvalid(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := d.Fs()
	manager := state.LocalManager()
	manifest := manager.Manifest().(*testdeps.Manifest)
	uow := manager.NewUnitOfWork(context.Background())

	// Init dir
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	inputDir := filesystem.Join(testDir, `..`, `..`, `fixtures`, `local`, `branch-invalid-meta-json`)
	assert.NoError(t, aferofs.CopyFs2Fs(nil, inputDir, fs, ``))

	// Setup manifest
	branchManifest := &model.BranchManifest{
		BranchKey: model.BranchKey{Id: 123},
		Paths: model.Paths{
			AbsPath: model.NewAbsPath(``, `main`),
		},
	}
	configManifest := &model.ConfigManifestWithRows{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: model.ConfigKey{BranchId: 123, ComponentId: `foo.bar`, Id: `456`},
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`main`, `config`),
			},
		},
	}
	assert.False(t, branchManifest.State().IsInvalid())
	assert.False(t, configManifest.State().IsInvalid())
	assert.False(t, branchManifest.State().IsNotFound())
	assert.False(t, configManifest.State().IsNotFound())
	assert.NoError(t, manifest.Records.SetRecords([]model.ObjectManifest{
		branchManifest,
		configManifest,
	}))

	// Load all
	uow.LoadAll(manager.Manifest(), model.NoFilter())

	// Invoke and check error
	// Branch is invalid, so config does not read at all (no error: config does not exist).
	err := uow.Invoke()
	expectedErr := `
branch metadata file "main/meta.json" is invalid:
  - invalid character 'f' looking for beginning of object key string, offset: 3
`
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())

	// Check manifest records
	assert.True(t, branchManifest.State().IsInvalid())
	assert.True(t, configManifest.State().IsInvalid())
	assert.False(t, branchManifest.State().IsNotFound())
	assert.False(t, configManifest.State().IsNotFound())
}
