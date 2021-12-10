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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestLoadRemoteStateEmpty(t *testing.T) {
	t.Parallel()
	m := createManifest(t)
	state, _, remoteErr := loadRemoteState(t, m, "empty.json")
	assert.NotNil(t, state)
	assert.Empty(t, remoteErr)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadRemoteStateComplex(t *testing.T) {
	t.Parallel()
	m := createManifest(t)
	state, envs, remoteErr := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	assert.Empty(t, remoteErr)
	assert.Equal(t, complexRemoteExpectedBranches(envs), state.Branches())
	assert.Equal(t, complexRemoteExpectedConfigs(envs), state.Configs())
	assert.Equal(t, complexRemoteExpectedConfigsRows(envs), state.ConfigRows())
}

func TestLoadRemoteStateAllowedBranches(t *testing.T) {
	t.Parallel()
	m := createManifest(t)
	m.Content.AllowedBranches = model.AllowedBranches{"f??"} // foo
	state, envs, remoteErr := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	assert.Empty(t, remoteErr)
	// Only Foo branch is loaded, other are "invisible"
	assert.Equal(t, []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
			},
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"",
						"foo",
					),
				},
			},
		},
	}, state.Branches())
}

func complexRemoteExpectedBranches(envs *env.Map) []*model.BranchState {
	return []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
				},
				Name:        "Bar",
				Description: "Bar branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"",
						"bar",
					),
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"",
						"foo",
					),
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"",
						"main",
					),
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
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Content:           orderedmap.New(),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"bar",
						"extractor/ex-generic-v2/empty",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`)),
				},
				Name:              "without-rows",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"bar",
						"extractor/ex-generic-v2/without-rows",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: fmt.Sprintf(`Copied from default branch configuration "empty" (%s) version 1`, envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				Content:           orderedmap.New(),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"foo",
						"extractor/ex-generic-v2/empty",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
				},
				Name:              "with-rows",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"foo",
						"extractor/keboola.ex-db-mysql/with-rows",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Name:              "empty",
				Description:       "test fixture",
				ChangeDescription: "created by test",
				Content:           orderedmap.New(),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"main",
						"extractor/ex-generic-v2/empty",
					),
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
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					Id:          model.RowId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`)),
				},
				Name:              "disabled",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
			ConfigRowManifest: &model.ConfigRowManifest{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					Id:          model.RowId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						`foo/extractor/keboola.ex-db-mysql/with-rows`,
						`rows/disabled`,
					),
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					Id:          model.RowId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`)),
				},
				Name:              "test_view",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
			ConfigRowManifest: &model.ConfigRowManifest{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					Id:          model.RowId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						`foo/extractor/keboola.ex-db-mysql/with-rows`,
						`rows/test-view`,
					),
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					Id:          model.RowId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`)),
				},
				Name:              "users",
				Description:       "test fixture",
				ChangeDescription: "created by test",
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
			ConfigRowManifest: &model.ConfigRowManifest{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentId: "keboola.ex-db-mysql",
					ConfigId:    model.ConfigId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					Id:          model.RowId(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						`foo/extractor/keboola.ex-db-mysql/with-rows`,
						`rows/users`,
					),
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

	// Force stable sort in tests
	m.SortBy = model.SortByPath
	m.Naming.Branch = "{branch_name}"
	m.Naming.Config = "{component_type}/{component_id}/{config_name}"
	m.Naming.ConfigRow = "rows/{config_row_name}"
	m.Naming.SharedCodeConfig = "_shared/{target_component_id}"
	m.Naming.SharedCodeConfigRow = "codes/{config_row_name}"
	m.Naming.VariablesConfig = "variables"
	m.Naming.VariablesValuesRow = "values/{config_row_name}"

	return m
}

func loadRemoteState(t *testing.T, m *manifest.Manifest, projectStateFile string) (*State, *env.Map, error) {
	t.Helper()

	envs := env.Empty()
	project := testproject.GetTestProject(t, envs)
	project.SetState(projectStateFile)

	logger, _ := utils.NewDebugLogger()
	state := newState(NewOptions(m, project.StorageApi(), project.SchedulerApi(), context.Background(), logger))
	remoteErr := state.loadRemoteState()
	return state, envs, remoteErr
}
