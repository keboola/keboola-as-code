package local_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestUnitOfWork_Load_NotFound(t *testing.T) {
	t.Parallel()
	_, uow, _, manifestInst := newTestUow(t)

	// Fixtures
	key := fixtures.MockedKey{Id: "123"}
	manifestInst.MustAdd(&fixtures.MockedManifest{MockedKey: key, PathValue: model.NewAbsPath("foo", "bar")})

	// Load
	uow.LoadAll()
	err := uow.Invoke()
	assert.Error(t, err)
	assert.Equal(t, "kind \"test\" not found", err.Error())
}

func TestUnitOfWork_Load_Mapper(t *testing.T) {
	t.Parallel()

	// Load objects
	testMapperInst := &testMapper{}
	m, fs := loadManifest(t, "minimal")
	s, err := loadState(t, m, fs, testMapperInst)
	assert.NoError(t, err)

	// Internal state has been mapped
	config := s.MustGet(model.ConfigKey{BranchId: 111, ComponentId: `ex-generic-v2`, Id: `456`}).(*model.Config)
	assert.Equal(t, `{"parameters":"overwritten","new":"value"}`, json.MustEncodeString(config.Content, false))

	// AfterLocalOperation event has been called
	assert.Equal(t, []string{
		`loaded branch "111"`,
		`loaded config "branch:111/component:ex-generic-v2/config:456"`,
	}, testMapperInst.localChanges)
}

func TestUnitOfWork_Load_Relations(t *testing.T) {
	t.Parallel()
	//d := dependencies.NewTestContainer()
	//logger := d.DebugLogger()
	//mockedState := d.EmptyState()
	//mockedState.Mapper().AddMapper(configmetadata.NewMapper(mockedState, d))
	//
	//configKey := model.ConfigKey{
	//	BranchId:    123,
	//	ComponentId: model.ComponentId("keboola.snowflake-transformation"),
	//	Id:          `456`,
	//}
	//configState := &model.ConfigState{
	//	ConfigManifest: &model.ConfigManifest{
	//		ConfigKey: configKey,
	//		Metadata: orderedmap.FromPairs([]orderedmap.Pair{
	//			{Key: "KBC.KaC.Meta1", Value: "val1"},
	//			{Key: "KBC.KaC.Meta2", Value: "val2"},
	//		}),
	//	},
	//	Local: &model.Config{
	//		ConfigKey: configKey,
	//		Name:      "My Config",
	//		Content:   orderedmap.New(),
	//	},
	//}
	//
	//recipe := model.NewLocalLoadRecipe(d.FileLoader(), configState.Manifest(), configState.Local)
	//assert.NoError(t, mockedState.Mapper().MapAfterLocalLoad(recipe))
	//assert.Empty(t, logger.WarnAndErrorMessages())
	//
	//config := recipe.Object.(*model.Config)
	//assert.NotEmpty(t, config.Metadata)
	//assert.Equal(t, "val1", config.Metadata["KBC.KaC.Meta1"])
	//assert.Equal(t, "val2", config.Metadata["KBC.KaC.Meta2"])
}

func TestUnitOfWork_Load_Minimal(t *testing.T) {
	t.Parallel()

	m, fs := loadManifest(t, "minimal")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Empty(t, localErr)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 1)
	assert.Empty(t, state.UntrackedPaths())
	assert.Equal(t, []string{
		"main",
		"main/description.md",
		"main/extractor",
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/description.md",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, state.TrackedPaths())
}

func TestUnitOfWork_Load_Complex(t *testing.T) {
	t.Parallel()

	m, fs := loadManifest(t, "complex")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Empty(t, localErr)
	assert.Equal(t, complexExpectedBranches(), utils.SortByName(state.Branches()))
	assert.Equal(t, complexExpectedConfigs(), utils.SortByName(state.Configs()))
	assert.Equal(t, complexExpectedConfigRows(), utils.SortByName(state.ConfigRows()))
	assert.Equal(t, []string{
		"123-branch/extractor/ex-generic-v2/456-todos/untracked1",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir/untracked2",
	}, state.UntrackedPaths())
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/description.md",
		"123-branch/extractor",
		"123-branch/extractor/ex-generic-v2",
		"123-branch/extractor/ex-generic-v2/456-todos",
		"123-branch/extractor/ex-generic-v2/456-todos/config.json",
		"123-branch/extractor/ex-generic-v2/456-todos/description.md",
		"123-branch/extractor/ex-generic-v2/456-todos/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/description.md",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/meta.json",
		"main",
		"main/description.md",
		"main/extractor",
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/description.md",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, state.TrackedPaths())
}

func TestUnitOfWork_Load_AllowedBranches(t *testing.T) {
	t.Parallel()

	m, fs := loadManifest(t, "minimal")
	m.SetAllowedBranches(model.AllowedBranches{"main"})
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Empty(t, localErr)
}

func TestUnitOfWork_Load_AllowedBranchesError(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "complex")
	m.SetAllowedBranches(model.AllowedBranches{"main"})
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Equal(t, `found manifest record for branch "123", but it is not allowed by the manifest definition`, localErr.Error())
}

func TestUnitOfWork_Load_BranchMissingMetaJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "branch-missing-meta-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing branch metadata file "main/meta.json"`, localErr.Error())
}

func TestUnitOfWork_Load_BranchMissingDescription(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "branch-missing-description")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing branch description file "main/description.md"`, localErr.Error())
}

func TestUnitOfWork_Load_ConfigMissingConfigJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-missing-config-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing config file "123-branch/extractor/ex-generic-v2/456-todos/config.json"`, localErr.Error())
}

func TestUnitOfWork_Load_ConfigMissingMetaJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-missing-meta-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing config metadata file "123-branch/extractor/ex-generic-v2/456-todos/meta.json"`, localErr.Error())
}

func TestUnitOfWork_Load_ConfigMissingDescription(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-missing-description")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing config description file "123-branch/extractor/ex-generic-v2/456-todos/description.md"`, localErr.Error())
}

func TestUnitOfWork_Load_ConfigRowMissingConfigJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-row-missing-config-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing config row file "123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/config.json"`, localErr.Error())
}

func TestUnitOfWork_Load_ConfigRowMissingMetaJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-row-missing-meta-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing config row metadata file "123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json"`, localErr.Error())
}

func TestUnitOfWork_Load_BranchInvalidMetaJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "branch-invalid-meta-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, "branch metadata file \"main/meta.json\" is invalid:\n  - invalid character 'f' looking for beginning of object key string, offset: 3", localErr.Error())
}

func TestUnitOfWork_Load_ConfigRowMissingDescription(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-row-missing-description")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, `missing config row description file "123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/description.md"`, localErr.Error())
}

func TestUnitOfWork_Load_ConfigInvalidConfigJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-invalid-config-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, "config file \"123-branch/extractor/ex-generic-v2/456-todos/config.json\" is invalid:\n  - invalid character 'f' looking for beginning of object key string, offset: 3", localErr.Error())
}

func TestUnitOfWork_Load_ConfigInvalidMetaJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-invalid-meta-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, "config metadata file \"123-branch/extractor/ex-generic-v2/456-todos/meta.json\" is invalid:\n  - invalid character 'f' looking for beginning of object key string, offset: 3", localErr.Error())
}

func TestUnitOfWork_Load_ConfigRowInvalidConfigJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-row-invalid-config-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, "config row file \"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json\" is invalid:\n  - invalid character 'f' looking for beginning of object key string, offset: 3", localErr.Error())
}

func TestUnitOfWork_Load_ConfigRowInvalidMetaJson(t *testing.T) {
	t.Parallel()
	m, fs := loadManifest(t, "config-row-invalid-meta-json")
	state, localErr := loadState(t, m, fs)
	assert.NotNil(t, state)
	assert.Error(t, localErr)
	assert.Equal(t, "config row metadata file \"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json\" is invalid:\n  - invalid character 'f' looking for beginning of object key string, offset: 3", localErr.Error())
}

func loadManifest(t *testing.T, projectDirName string) (*manifest.Manifest, filesystem.Fs) {
	t.Helper()

	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_PROJECT_ID", "12345")
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	envs.Set("LOCAL_STATE_MY_BRANCH_ID", "123")
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", "456")
	envs.Set("LOCAL_STATE_MYSQL_CONFIG_ID", "896")

	// Objects dir
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	stateDir := filesystem.Join(testDir, "..", "fixtures", "local", projectDirName)

	// Create Fs
	fs := testfs.NewMemoryFsFrom(stateDir)
	testhelper.ReplaceEnvsDir(fs, `/`, envs)

	// Load manifest
	manifestInst, err := project.LoadManifest(context.Background(), fs, false)
	assert.NoError(t, err)

	return manifestInst, fs
}

func loadState(t *testing.T, manifestInst *manifest.Manifest, fs filesystem.Fs, mappers ...interface{}) (*local.State, error) {
	t.Helper()

	// Dependencies
	d := dependencies.NewTestContainer()
	d.SetFs(fs)
	d.UseMockedStorageApi()
	d.UseMockedSchedulerApi()

	// Mappers
	mappersFactory := project.LocalMappers(d)
	if len(mappers) > 0 {
		// Replace default mappers
		mappersFactory = func(state *local.State) (mapper.Mappers, error) {
			return mappers, nil
		}
	}

	// Create state
	s, err := local.NewState(d, fs, manifestInst, mappersFactory)
	if err != nil {
		return nil, err
	}

	// Load state
	uow := s.NewUnitOfWork(context.Background(), manifestInst.Filter())
	uow.LoadAll()
	return s, uow.Invoke()
}

func complexExpectedBranches() []*model.Branch {
	return []*model.Branch{
		{
			BranchKey:   model.BranchKey{Id: 123},
			Name:        "Branch",
			Description: "My branch",
			IsDefault:   false,
		},
		{
			BranchKey:   model.BranchKey{Id: 111},
			Name:        "Main",
			Description: "Main branch",
			IsDefault:   true,
		},
	}
}

func complexExpectedConfigs() []*model.Config {
	return []*model.Config{
		{
			ConfigKey: model.ConfigKey{
				BranchId:    123,
				ComponentId: "keboola.ex-db-mysql",
				Id:          "896",
			},
			Name:              "tables",
			Description:       "tables config",
			ChangeDescription: "",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "parameters",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key: "db",
							Value: orderedmap.FromPairs([]orderedmap.Pair{
								{
									Key:   "host",
									Value: "mysql.example.com",
								},
							}),
						},
					}),
				},
			}),
			Metadata: make(map[string]string),
		},
		{
			ConfigKey: model.ConfigKey{
				BranchId:    111,
				ComponentId: "ex-generic-v2",
				Id:          "456",
			},
			Name:              "todos",
			Description:       "todos config",
			ChangeDescription: "",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "parameters",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key: "api",
							Value: orderedmap.FromPairs([]orderedmap.Pair{
								{
									Key:   "baseUrl",
									Value: "https://jsonplaceholder.typicode.com",
								},
							}),
						},
					}),
				},
			}),
			Metadata: make(map[string]string),
		},
		{
			ConfigKey: model.ConfigKey{
				BranchId:    123,
				ComponentId: "ex-generic-v2",
				Id:          "456",
			},
			Name:              "todos",
			Description:       "todos config",
			ChangeDescription: "",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "parameters",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key: "api",
							Value: orderedmap.FromPairs([]orderedmap.Pair{
								{
									Key:   "baseUrl",
									Value: "https://jsonplaceholder.typicode.com",
								},
							}),
						},
					}),
				},
			}),
			Metadata: make(map[string]string),
		},
	}
}

func complexExpectedConfigRows() []*model.ConfigRow {
	return []*model.ConfigRow{
		{
			ConfigRowKey: model.ConfigRowKey{
				BranchId:    123,
				ComponentId: "keboola.ex-db-mysql",
				ConfigId:    "896",
				Id:          "56",
			},
			Name:              "disabled",
			Description:       "",
			ChangeDescription: "",
			IsDisabled:        true,
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "parameters",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "incremental", Value: false},
					}),
				},
			}),
		},
		{
			ConfigRowKey: model.ConfigRowKey{
				BranchId:    123,
				ComponentId: "keboola.ex-db-mysql",
				ConfigId:    "896",
				Id:          "34",
			},
			Name:              "test_view",
			Description:       "row description",
			ChangeDescription: "",
			IsDisabled:        false,
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "parameters",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "incremental", Value: false},
					}),
				},
			}),
		},
		{
			ConfigRowKey: model.ConfigRowKey{
				BranchId:    123,
				ComponentId: "keboola.ex-db-mysql",
				ConfigId:    "896",
				Id:          "12",
			},
			Name:              "users",
			Description:       "",
			ChangeDescription: "",
			IsDisabled:        false,
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "parameters",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "incremental", Value: false},
					}),
				},
			}),
		},
	}
}
