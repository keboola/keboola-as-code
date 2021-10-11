package state

import (
	"context"
	"fmt"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLoadRemoteStateEmpty(t *testing.T) {
	m := createManifest(t)
	state, _ := loadRemoteState(t, m, "empty.json")
	assert.NotNil(t, state)
	assert.Empty(t, state.RemoteErrors().Errors)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadRemoteStateComplex(t *testing.T) {
	m := createManifest(t)
	state, envs := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	assert.Empty(t, state.RemoteErrors().Errors)
	assert.Equal(t, complexRemoteExpectedBranches(envs), utils.SortByName(state.Branches()))
	assert.Equal(t, complexRemoteExpectedConfigs(envs), utils.SortByName(state.Configs()))
	assert.Equal(t, complexRemoteExpectedConfigsRows(envs), utils.SortByName(state.ConfigRows()))
}

func TestLoadRemoteStateAllowedBranches(t *testing.T) {
	m := createManifest(t)
	m.Content.AllowedBranches = model.AllowedBranches{"f??"} // foo
	state, envs := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	assert.Empty(t, state.RemoteErrors().Errors)
	// Only Foo branch is loaded, other are "invisible"
	assert.Equal(t, []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
			},
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: envs.MustGet(`TEST_BRANCH_FOO_ID`) + "-foo",
						ParentPath: "",
					},
				},
			},
		},
	}, utils.SortByName(state.Branches()))
}

func complexRemoteExpectedBranches(envs *env.Map) []*model.BranchState {
	return []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`)),
				},
				Name:        "Bar",
				Description: "Bar branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: envs.MustGet(`TEST_BRANCH_BAR_ID`) + "-bar",
						ParentPath: "",
					},
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: envs.MustGet(`TEST_BRANCH_FOO_ID`) + "-foo",
						ParentPath: "",
					},
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "main",
						ParentPath: "",
					},
				},
			},
		},
	}
}

func complexRemoteExpectedConfigs(envs *env.Map) []*model.ConfigState {
	return []*model.ConfigState{
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: "created by test",
				Content:           utils.NewOrderedMap(),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "extractor/ex-generic-v2/" + envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
						ParentPath: "main",
					},
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Content:           utils.NewOrderedMap(),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "extractor/ex-generic-v2/" + envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
						ParentPath: envs.MustGet(`TEST_BRANCH_FOO_ID`) + "-foo",
					},
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Content:           utils.NewOrderedMap(),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "extractor/ex-generic-v2/" + envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`) + "-empty",
						ParentPath: envs.MustGet(`TEST_BRANCH_BAR_ID`) + "-bar",
					},
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "extractor/keboola.ex-db-mysql/" + envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
						ParentPath: envs.MustGet(`TEST_BRANCH_FOO_ID`) + "-foo",
					},
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`),
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "extractor/ex-generic-v2/" + envs.MustGet(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`) + "-without-rows",
						ParentPath: envs.MustGet(`TEST_BRANCH_BAR_ID`) + "-bar",
					},
				},
			},
		},
	}
}

func complexRemoteExpectedConfigsRows(envs *env.Map) []*model.ConfigRowState {
	return []*model.ConfigRowState{
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`),
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
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: `rows/` + envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`) + "-disabled",
						ParentPath: envs.MustGet(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
					},
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`),
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
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: `rows/` + envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`) + "-test-view",
						ParentPath: envs.MustGet(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
					},
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`),
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
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`)),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`),
					Id:          envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: `rows/` + envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`) + "-users",
						ParentPath: envs.MustGet(`TEST_BRANCH_FOO_ID`) + "-foo/extractor/keboola.ex-db-mysql/" + envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`) + "-with-rows",
					},
				},
			},
		},
	}
}

func createManifest(t *testing.T) *manifest.Manifest {
	t.Helper()

	fs, err := aferofs.NewMemoryFs(zap.NewNop().Sugar(), ".")
	assert.NoError(t, err)
	m, err := manifest.NewManifest(1, "connection.keboola.com", fs)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	return m
}

func loadRemoteState(t *testing.T, m *manifest.Manifest, projectStateFile string) (*State, *env.Map) {
	t.Helper()

	envs := env.Empty()
	project := testproject.GetTestProject(t, envs)
	project.SetState(projectStateFile)

	logger, _ := utils.NewDebugLogger()
	state := newState(NewOptions(m, project.Api(), context.Background(), logger))
	state.doLoadRemoteState()
	return state, envs
}
