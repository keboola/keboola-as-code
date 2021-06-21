package state

import (
	"context"
	"fmt"
	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/fixtures"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

func TestLoadRemoteStateEmpty(t *testing.T) {
	remote.SetStateOfTestProject(t, "empty.json")

	projectDir := t.TempDir()
	api, _ := remote.TestStorageApiWithToken(t)
	state := model.NewState(projectDir)
	err := LoadRemoteState(state, context.Background(), api)
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadRemoteStateComplex(t *testing.T) {
	remote.SetStateOfTestProject(t, "complex.json")

	projectDir := t.TempDir()
	api, _ := remote.TestStorageApiWithToken(t)
	state := model.NewState(projectDir)
	err := LoadRemoteState(state, context.Background(), api)
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())
	assert.Equal(t, complexRemoteExpectedBranches(), state.Branches())
	assert.Equal(t, complexRemoteExpectedConfigs(), state.Configs())
}

// TestDumpProjectState dumps test project as JSON file
// Result file is ignored in .gitignore
func TestDumpProjectState(t *testing.T) {
	// Load remote state and convert
	projectDir := t.TempDir()
	api, _ := remote.TestStorageApiWithToken(t)
	state := model.NewState(projectDir)
	stateErr := LoadRemoteState(state, context.Background(), api)
	assert.NotNil(t, stateErr)
	if stateErr.Len() > 0 {
		assert.FailNow(t, "%s", stateErr)
	}

	s, err := fixtures.ConvertRemoteStateToFixtures(state)
	if err != nil {
		assert.FailNow(t, "%s", err)
	}

	// Convert to JSON
	data, err := json.Encode(s, true)
	assert.NoError(t, err)

	// Replace secrets, eg. "#password": "KBC::P..." -> "#password": "my-secret"
	reg := regexp.MustCompile(`(\s*"#[^"]+": ")KBC::[^"]+(")`)
	data = reg.ReplaceAll(data, []byte("${1}my-secret${2}"))

	// Write
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	filePath := filepath.Join(testDir, "..", "fixtures", "remote", "current_project_state.json")
	assert.NoError(t, os.WriteFile(filePath, data, 0666))
	fmt.Printf("Dumped to the file \"%s\"\n", filePath)
}

func complexRemoteExpectedBranches() []*model.BranchState {
	return []*model.BranchState{
		{
			Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
			Remote: &model.Branch{
				Id:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "main",
					ParentPath: "",
				},
				Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
			},
		},
		{
			Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
			Remote: &model.Branch{
				Id:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo",
					ParentPath: "",
				},
				Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
			},
		},
		{
			Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
			Remote: &model.Branch{
				Id:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
				Name:        "Bar",
				Description: "Bar branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       utils.MustGetEnv(`TEST_BRANCH_BAR_ID`) + "-bar",
					ParentPath: "",
				},
				Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
			},
		},
	}
}

func complexRemoteExpectedConfigs() []*model.ConfigState {
	return []*model.ConfigState{
		{
			BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
			ComponentId: "ex-generic-v2",
			Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
			Remote: &model.Config{
				BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				ComponentId:       "ex-generic-v2",
				Id:                utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: "created by test",
				Config:            orderedmap.New(),
				Rows:              []*model.ConfigRow{},
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
					ParentPath: "main",
				},
				BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				ComponentId: "ex-generic-v2",
				Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
			},
		},
		{
			BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
			ComponentId: "ex-generic-v2",
			Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
			Remote: &model.Config{
				BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				ComponentId:       "ex-generic-v2",
				Id:                utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Config:            orderedmap.New(),
				Rows:              []*model.ConfigRow{},
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo",
				},
				BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				ComponentId: "ex-generic-v2",
				Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
			},
		},
		{
			BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
			ComponentId: "keboola.ex-db-mysql",
			Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
			Remote: &model.Config{
				BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				ComponentId:       "keboola.ex-db-mysql",
				Id:                utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
				Name:              "with-rows",
				Description:       "test fixture",
				ChangeDescription: "created by test",
				Config: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "db",
								Value: utils.PairsToOrderedMap([]utils.Pair{
									{
										Key:   "host",
										Value: "mysql.example.com",
									},
								}),
							},
						}),
					},
				}),
				Rows: []*model.ConfigRow{
					{
						BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
						ComponentId:       "keboola.ex-db-mysql",
						ConfigId:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
						Id:                utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`),
						Name:              "disabled",
						Description:       "test fixture",
						ChangeDescription: "created by test",
						IsDisabled:        true,
						Config: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "parameters",
								Value: utils.PairsToOrderedMap([]utils.Pair{
									{Key: "incremental", Value: "false"},
								}),
							},
						}),
					},
					{
						BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
						ComponentId:       "keboola.ex-db-mysql",
						ConfigId:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
						Id:                utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`),
						Name:              "test_view",
						Description:       "test fixture",
						ChangeDescription: "created by test",
						IsDisabled:        false,
						Config: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "parameters",
								Value: utils.PairsToOrderedMap([]utils.Pair{
									{Key: "incremental", Value: "false"},
								}),
							},
						}),
					},
					{
						BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
						ComponentId:       "keboola.ex-db-mysql",
						ConfigId:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
						Id:                utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`),
						Name:              "users",
						Description:       "test fixture",
						ChangeDescription: "created by test",
						IsDisabled:        false,
						Config: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "parameters",
								Value: utils.PairsToOrderedMap([]utils.Pair{
									{Key: "incremental", Value: "false"},
								}),
							},
						}),
					},
				},
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo",
				},
				BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
				ComponentId: "keboola.ex-db-mysql",
				Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
				Rows: []*model.ConfigRowManifest{
					{
						ManifestPaths: model.ManifestPaths{
							Path:       utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`) + "-disabled",
							ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows/rows",
						},
						BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
						ComponentId: "keboola.ex-db-mysql",
						ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
						Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`),
					},
					{
						ManifestPaths: model.ManifestPaths{
							Path:       utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`) + "-test-view",
							ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows/rows",
						},
						BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
						ComponentId: "keboola.ex-db-mysql",
						ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
						Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`),
					},
					{
						ManifestPaths: model.ManifestPaths{
							Path:       utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`) + "-users",
							ParentPath: utils.MustGetEnv(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows/rows",
						},
						BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`)),
						ComponentId: "keboola.ex-db-mysql",
						ConfigId:    utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
						Id:          utils.MustGetEnv(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`),
					},
				},
			},
		},
		{
			BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
			ComponentId: "ex-generic-v2",
			Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
			Remote: &model.Config{
				BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
				ComponentId:       "ex-generic-v2",
				Id:                utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Config:            orderedmap.New(),
				Rows:              []*model.ConfigRow{},
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_BAR_ID`) + "-bar",
				},
				BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
				ComponentId: "ex-generic-v2",
				Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
			},
		},
		{
			BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
			ComponentId: "ex-generic-v2",
			Id:          utils.MustGetEnv(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`),
			Remote: &model.Config{
				BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
				ComponentId:       "ex-generic-v2",
				Id:                utils.MustGetEnv(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`),
				Name:              "without-rows",
				Description:       "test fixture",
				ChangeDescription: "created by test",
				Config: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key: "parameters",
						Value: utils.PairsToOrderedMap([]utils.Pair{
							{
								Key: "api",
								Value: utils.PairsToOrderedMap([]utils.Pair{
									{
										Key:   "baseUrl",
										Value: "https://jsonplaceholder.typicode.com",
									},
								}),
							},
						}),
					},
				}),
				Rows: []*model.ConfigRow{},
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "extractor/ex-generic-v2/" + utils.MustGetEnv(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`) + "-without-rows",
					ParentPath: utils.MustGetEnv(`TEST_BRANCH_BAR_ID`) + "-bar",
				},
				BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`)),
				ComponentId: "ex-generic-v2",
				Id:          utils.MustGetEnv(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`),
			},
		},
	}
}
