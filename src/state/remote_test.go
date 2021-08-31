package state

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
)

func TestLoadRemoteStateEmpty(t *testing.T) {
	m := createManifest(t)
	state := loadRemoteState(t, m, "empty.json")
	assert.NotNil(t, state)
	assert.Empty(t, state.RemoteErrors().Errors)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadRemoteStateComplex(t *testing.T) {
	m := createManifest(t)
	state := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	assert.Empty(t, state.RemoteErrors().Errors)
	assert.Equal(t, complexRemoteExpectedBranches(), utils.SortByName(state.Branches()))
	assert.Equal(t, complexRemoteExpectedConfigs(), utils.SortByName(state.Configs()))
	assert.Equal(t, complexRemoteExpectedConfigsRows(), utils.SortByName(state.ConfigRows()))
}

func TestLoadRemoteStateAllowedBranches(t *testing.T) {
	m := createManifest(t)
	m.Content.AllowedBranches = model.AllowedBranches{"f??"} // foo
	state := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	assert.Empty(t, state.RemoteErrors().Errors)
	// Only Foo branch is loaded, other are "invisible"
	assert.Equal(t, []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
			},
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				},
				Paths: model.Paths{
					Path:       utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo",
					ParentPath: "",
				},
			},
		},
	}, utils.SortByName(state.Branches()))
}

func complexRemoteExpectedBranches() []*model.BranchState {
	return []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
				},
				Name:        "Bar",
				Description: "Bar branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
				},
				Paths: model.Paths{
					Path:       utils.MustGetEnv(`TEST_BRANCH_BAR_ID`) + "-bar",
					ParentPath: "",
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				},
				Paths: model.Paths{
					Path:       utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo",
					ParentPath: "",
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				},
				Paths: model.Paths{
					Path:       "main",
					ParentPath: "",
				},
			},
		},
	}
}

func complexRemoteExpectedConfigs() []*model.ConfigState {
	return []*model.ConfigState{
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: "created by test",
				Content:           utils.NewOrderedMap(),
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
					ParentPath: "main",
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Content:           utils.NewOrderedMap(),
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo",
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Content:           utils.NewOrderedMap(),
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_BAR_ID`) + "-bar",
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
				},
				Name:              "with-rows",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
				},
				Paths: model.Paths{
					Path:       "extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo",
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`),
				},
				Name:              "without-rows",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`),
				},
				Paths: model.Paths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`) + "-without-rows",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_BAR_ID`) + "-bar",
				},
			},
		},
	}
}

func complexRemoteExpectedConfigsRows() []*model.ConfigRowState {
	return []*model.ConfigRowState{
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`),
				},
				Name:              "disabled",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`),
				},
				Paths: model.Paths{
					Path:       `rows/` + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`) + "-disabled",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`),
				},
				Name:              "test_view",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`),
				},
				Paths: model.Paths{
					Path:       `rows/` + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`) + "-test-view",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`),
				},
				Name:              "users",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`),
				},
				Paths: model.Paths{
					Path:       `rows/` + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`) + "-users",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
				},
			},
		},
	}
}

func createManifest(t *testing.T) *manifest.Manifest {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	m, err := manifest.NewManifest(1, "connection.keboola.com", projectDir, metadataDir)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	return m
}

func loadRemoteState(t *testing.T, m *manifest.Manifest, projectStateFile string) *State {
	api, _ := remote.TestStorageApiWithToken(t)
	remote.SetStateOfTestProject(t, api, projectStateFile)
	logger, _ := utils.NewDebugLogger()
	state := newState(NewOptions(m, api, context.Background(), logger))
	state.doLoadRemoteState()
	return state
}
