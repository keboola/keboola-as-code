package state

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
)

func TestLoadLocalStateMinimal(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "minimal")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Empty(t, state.LocalErrors().Errors)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 1)
	assert.Empty(t, state.UntrackedPaths())
	assert.Equal(t, []string{
		"main",
		"main/extractor",
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, state.TrackedPaths())
}

func TestLoadLocalStateComplex(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "complex")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Empty(t, state.LocalErrors().Errors)
	assert.Equal(t, complexLocalExpectedBranches(), utils.SortByName(state.Branches()))
	assert.Equal(t, complexLocalExpectedConfigs(), utils.SortByName(state.Configs()))
	assert.Equal(t, complexLocalExpectedConfigRows(), utils.SortByName(state.ConfigRows()))
	assert.Equal(t, []string{
		"123-branch/extractor/ex-generic-v2/456-todos/untracked1",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir",
		"123-branch/extractor/keboola.ex-db-mysql/untrackedDir/untracked2",
	}, state.UntrackedPaths())
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/extractor",
		"123-branch/extractor/ex-generic-v2",
		"123-branch/extractor/ex-generic-v2/456-todos",
		"123-branch/extractor/ex-generic-v2/456-todos/config.json",
		"123-branch/extractor/ex-generic-v2/456-todos/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/meta.json",
		"main",
		"main/extractor",
		"main/extractor/ex-generic-v2",
		"main/extractor/ex-generic-v2/456-todos",
		"main/extractor/ex-generic-v2/456-todos/config.json",
		"main/extractor/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, state.TrackedPaths())
}

func TestLoadLocalStateAllowedBranches(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "minimal")
	m.Content.AllowedBranches = model.AllowedBranches{"main"}
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Empty(t, state.LocalErrors().Errors)
}

func TestLoadLocalStateAllowedBranchesError(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "complex")
	m.Content.AllowedBranches = model.AllowedBranches{"main"}
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Equal(t, `found manifest record for branch "123", but it is not allowed by the manifest definition`, state.LocalErrors().Error())
}

func TestLoadLocalStateBranchMissingMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "branch-missing-meta-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, `missing branch metadata file "main/meta.json"`, state.LocalErrors().Error())
}

func TestLoadLocalStateConfigMissingConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-missing-config-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, `missing config file "123-branch/extractor/ex-generic-v2/456-todos/config.json"`, state.LocalErrors().Error())
}

func TestLoadLocalStateConfigMissingMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-missing-meta-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, `missing config metadata file "123-branch/extractor/ex-generic-v2/456-todos/meta.json"`, state.LocalErrors().Error())
}

func TestLoadLocalStateConfigRowMissingConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-row-missing-config-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, `missing config row file "123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/config.json"`, state.LocalErrors().Error())
}

func TestLoadLocalStateConfigRowMissingMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-row-missing-meta-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, `missing config row metadata file "123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json"`, state.LocalErrors().Error())
}

func TestLoadLocalStateBranchInvalidMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "branch-invalid-meta-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, "branch metadata file \"main/meta.json\" is invalid:\n\t- invalid character 'f' looking for beginning of object key string, offset: 3", state.LocalErrors().Error())
}

func TestLoadLocalStateConfigInvalidConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-invalid-config-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, "config file \"123-branch/extractor/ex-generic-v2/456-todos/config.json\" is invalid:\n\t- invalid character 'f' looking for beginning of object key string, offset: 3", state.LocalErrors().Error())
}

func TestLoadLocalStateConfigInvalidMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-invalid-meta-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, "config metadata file \"123-branch/extractor/ex-generic-v2/456-todos/meta.json\" is invalid:\n\t- invalid character 'f' looking for beginning of object key string, offset: 3", state.LocalErrors().Error())
}

func TestLoadLocalStateConfigRowInvalidConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-row-invalid-config-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, "config row file \"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json\" is invalid:\n\t- invalid character 'f' looking for beginning of object key string, offset: 3", state.LocalErrors().Error())
}

func TestLoadLocalStateConfigRowInvalidMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	m := loadManifest(t, "config-row-invalid-meta-json")
	state := loadLocalTestState(t, m)
	assert.NotNil(t, state)
	assert.Greater(t, state.LocalErrors().Len(), 0)
	assert.Equal(t, "config row metadata file \"123-branch/extractor/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json\" is invalid:\n\t- invalid character 'f' looking for beginning of object key string, offset: 3", state.LocalErrors().Error())
}

func loadLocalTestState(t *testing.T, m *manifest.Manifest) *State {
	// Mocked API
	logger, _ := utils.NewDebugLogger()
	api, _ := remote.TestMockedStorageApi(t)

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":                     "ex-generic-v2",
		"type":                   "extractor",
		"name":                   "Generic",
		"configurationSchema":    map[string]interface{}{},
		"configurationRowSchema": map[string]interface{}{},
	})
	assert.NoError(t, err)
	getMySqlExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":                     "keboola.ex-db-mysql",
		"type":                   "extractor",
		"name":                   "MySQL",
		"configurationSchema":    map[string]interface{}{},
		"configurationRowSchema": map[string]interface{}{},
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)
	httpmock.RegisterResponder("GET", `=~/storage/components/keboola.ex-db-mysql`, getMySqlExResponder)

	// Load state
	options := NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	state, _ := LoadState(options)
	return state
}

func loadManifest(t *testing.T, projectDirName string) *manifest.Manifest {
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	utils.MustSetEnv("LOCAL_STATE_MY_BRANCH_ID", "123")
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", "456")
	utils.MustSetEnv("LOCAL_STATE_MYSQL_CONFIG_ID", "896")

	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	stateDir := filepath.Join(testDir, "..", "fixtures", "local", projectDirName)
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)

	// Copy test data
	err := copy.Copy(stateDir, projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	utils.ReplaceEnvsDir(projectDir, nil)

	// Load manifest
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	return m
}

func complexLocalExpectedBranches() []*model.BranchState {
	return []*model.BranchState{
		{
			Local: &model.Branch{
				BranchKey: model.BranchKey{
					Id: 123,
				},
				Name:        "Branch",
				Description: "My branch",
				IsDefault:   false,
			},
			BranchManifest: &model.BranchManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				BranchKey: model.BranchKey{
					Id: 123,
				},
				Paths: model.Paths{
					Path:         "123-branch",
					ParentPath:   "",
					RelatedPaths: []string{model.MetaFile},
				},
			},
		},
		{
			Local: &model.Branch{
				BranchKey: model.BranchKey{
					Id: 111,
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			BranchManifest: &model.BranchManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				BranchKey: model.BranchKey{
					Id: 111,
				},
				Paths: model.Paths{
					Path:         "main",
					ParentPath:   "",
					RelatedPaths: []string{model.MetaFile},
				},
			},
		},
	}
}

func complexLocalExpectedConfigs() []*model.ConfigState {
	return []*model.ConfigState{
		{
			Local: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    123,
					ComponentId: "keboola.ex-db-mysql",
					Id:          "896",
				},
				Name:              "tables",
				Description:       "tables config",
				ChangeDescription: "",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: *utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "db",
								Value: *utils.PairsToOrderedMap([]utils.Pair{
									{
										Key:   "host",
										Value: "mysql.example.com",
									},
								}),
							},
						}),
					},
				}),
			},
			Component: &model.Component{
				ComponentKey: model.ComponentKey{
					Id: "keboola.ex-db-mysql",
				},
				Type:      "extractor",
				Name:      "MySQL",
				Schema:    map[string]interface{}{},
				SchemaRow: map[string]interface{}{},
			},
			ConfigManifest: &model.ConfigManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigKey: model.ConfigKey{
					BranchId:    123,
					ComponentId: "keboola.ex-db-mysql",
					Id:          "896",
				},
				Paths: model.Paths{
					Path:         "extractor/keboola.ex-db-mysql/896-tables",
					ParentPath:   "123-branch",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
				},
			},
		},
		{
			Local: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    111,
					ComponentId: "ex-generic-v2",
					Id:          "456",
				},
				Name:              "todos",
				Description:       "todos config",
				ChangeDescription: "",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: *utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "api",
								Value: *utils.PairsToOrderedMap([]utils.Pair{
									{
										Key:   "baseUrl",
										Value: "https://jsonplaceholder.typicode.com",
									},
								}),
							},
						}),
					},
				}),
			},
			Component: &model.Component{
				ComponentKey: model.ComponentKey{
					Id: "ex-generic-v2",
				},
				Type:      "extractor",
				Name:      "Generic",
				Schema:    map[string]interface{}{},
				SchemaRow: map[string]interface{}{},
			},
			ConfigManifest: &model.ConfigManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigKey: model.ConfigKey{
					BranchId:    111,
					ComponentId: "ex-generic-v2",
					Id:          "456",
				},
				Paths: model.Paths{
					Path:         "extractor/ex-generic-v2/456-todos",
					ParentPath:   "main",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
				},
			},
		},
		{
			Local: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    123,
					ComponentId: "ex-generic-v2",
					Id:          "456",
				},
				Name:              "todos",
				Description:       "todos config",
				ChangeDescription: "",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: *utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "api",
								Value: *utils.PairsToOrderedMap([]utils.Pair{
									{
										Key:   "baseUrl",
										Value: "https://jsonplaceholder.typicode.com",
									},
								}),
							},
						}),
					},
				}),
			},
			Component: &model.Component{
				ComponentKey: model.ComponentKey{
					Id: "ex-generic-v2",
				},
				Type:      "extractor",
				Name:      "Generic",
				Schema:    map[string]interface{}{},
				SchemaRow: map[string]interface{}{},
			},
			ConfigManifest: &model.ConfigManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigKey: model.ConfigKey{
					BranchId:    123,
					ComponentId: "ex-generic-v2",
					Id:          "456",
				},
				Paths: model.Paths{
					Path:         "extractor/ex-generic-v2/456-todos",
					ParentPath:   "123-branch",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
				},
			},
		},
	}
}

func complexLocalExpectedConfigRows() []*model.ConfigRowState {
	return []*model.ConfigRowState{
		{
			Local: &model.ConfigRow{
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
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: *utils.PairsToOrderedMap([]utils.Pair{
							{Key: "incremental", Value: false},
						}),
					},
				}),
			},
			ConfigRowManifest: &model.ConfigRowManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    123,
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    "896",
					Id:          "56",
				},
				Paths: model.Paths{
					Path:         "rows/56-disabled",
					ParentPath:   "123-branch/extractor/keboola.ex-db-mysql/896-tables",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
				},
			},
		},
		{
			Local: &model.ConfigRow{
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
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: *utils.PairsToOrderedMap([]utils.Pair{
							{Key: "incremental", Value: false},
						}),
					},
				}),
			},
			ConfigRowManifest: &model.ConfigRowManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    123,
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    "896",
					Id:          "34",
				},
				Paths: model.Paths{
					Path:         "rows/34-test-view",
					ParentPath:   "123-branch/extractor/keboola.ex-db-mysql/896-tables",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
				},
			},
		},
		{
			Local: &model.ConfigRow{
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
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: *utils.PairsToOrderedMap([]utils.Pair{
							{Key: "incremental", Value: false},
						}),
					},
				}),
			},
			ConfigRowManifest: &model.ConfigRowManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    123,
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    "896",
					Id:          "12",
				},
				Paths: model.Paths{
					Path:         "rows/12-users",
					ParentPath:   "123-branch/extractor/keboola.ex-db-mysql/896-tables",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
				},
			},
		},
	}
}
