package corefiles_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLoadCoreFiles(t *testing.T) {
	t.Parallel()
	state := createStateWithMapper(t)
	fs := state.ObjectsRoot()
	ctx := t.Context()

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
	require.NoError(t, fs.Mkdir(ctx, manifest.Path()))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(state.NamingGenerator().MetaFilePath(manifest.Path()), metaFile)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(state.NamingGenerator().ConfigFilePath(manifest.Path()), configFile)))

	// Call mapper
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), manifest, object)
	require.NoError(t, state.Mapper().MapAfterLocalLoad(t.Context(), recipe))

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
	state := createStateWithMapper(t)
	fs := state.ObjectsRoot()
	manager := state.LocalManager()
	manifest := manager.Manifest().(*fixtures.Manifest)
	uow := manager.NewUnitOfWork(t.Context())

	// Init dir
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	inputDir := filesystem.Join(testDir, `..`, `..`, `fixtures`, `local`, `branch-invalid-meta-json`)
	require.NoError(t, aferofs.CopyFs2Fs(nil, inputDir, fs, ``))

	// Setup manifest
	branchManifest := &model.BranchManifest{
		BranchKey: model.BranchKey{ID: 123},
		Paths: model.Paths{
			AbsPath: model.NewAbsPath(``, `main`),
		},
	}
	configManifest := &model.ConfigManifestWithRows{
		ConfigManifest: model.ConfigManifest{
			ConfigKey: model.ConfigKey{BranchID: 123, ComponentID: `foo.bar`, ID: `456`},
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`main`, `config`),
			},
		},
	}
	assert.False(t, branchManifest.State().IsInvalid())
	assert.False(t, configManifest.State().IsInvalid())
	assert.False(t, branchManifest.State().IsNotFound())
	assert.False(t, configManifest.State().IsNotFound())
	require.NoError(t, manifest.Records.SetRecords([]model.ObjectManifest{
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
	require.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())

	// Check manifest records
	assert.True(t, branchManifest.State().IsInvalid())
	assert.True(t, configManifest.State().IsInvalid())
	assert.False(t, branchManifest.State().IsNotFound())
	assert.False(t, configManifest.State().IsNotFound())
}
