package state_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestLoadRemoteStateEmpty(t *testing.T) {
	t.Parallel()
	m := createManifest(t)
	state, _, remoteErr := loadRemoteState(t, m, "empty.json")
	assert.NotNil(t, state)
	require.NoError(t, remoteErr)
	assert.Len(t, state.Branches(), 1)
	assert.Empty(t, state.Configs())
}

func TestLoadRemoteStateComplex(t *testing.T) {
	t.Parallel()
	m := createManifest(t)
	state, envs, remoteErr := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	require.NoError(t, remoteErr)
	assert.Equal(t, complexRemoteExpectedBranches(envs), state.Branches())
	assert.Equal(t, complexRemoteExpectedConfigs(envs), state.Configs())
	assert.Equal(t, complexRemoteExpectedConfigsRows(envs), state.ConfigRows())
}

func TestLoadRemoteStateAllowedBranches(t *testing.T) {
	t.Parallel()
	m := createManifest(t)
	m.SetAllowedBranches(model.AllowedBranches{"f??"}) // foo
	state, envs, remoteErr := loadRemoteState(t, m, "complex.json")
	assert.NotNil(t, state)
	require.NoError(t, remoteErr)
	// Only Foo branch is loaded, other are "invisible"
	assert.Equal(t, []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
				Metadata:    make(map[string]string),
			},
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
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
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
				},
				Name:        "Bar",
				Description: "Bar branch",
				IsDefault:   false,
				Metadata:    make(map[string]string),
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"",
						"bar",
					),
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Name:        "Foo",
				Description: "Foo branch",
				IsDefault:   false,
				Metadata:    make(map[string]string),
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"",
						"foo",
					),
				},
			},
		},
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
				Metadata:    make(map[string]string),
			},
			// Generated manifest
			BranchManifest: &model.BranchManifest{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
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
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Name:        "empty",
				Description: "test fixture",
				Content:     orderedmap.New(),
				Metadata:    make(map[string]string),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"bar",
						"extractor/ex-generic-v2/empty",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`)),
				},
				Name:        "without-rows",
				Description: "test fixture",
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_BAR_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"bar",
						"extractor/ex-generic-v2/without-rows",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Name:        "empty",
				Description: "test fixture",
				Content:     orderedmap.New(),
				Metadata:    make(map[string]string),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"foo",
						"extractor/ex-generic-v2/empty",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
				},
				Name:        "with-rows",
				Description: "test fixture",
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
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"foo",
						"extractor/keboola.ex-db-mysql/with-rows",
					),
				},
			},
		},
		{
			Remote: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Name:        "empty",
				Description: "test fixture",
				Content:     orderedmap.New(),
				Metadata:    make(map[string]string),
			},
			// Generated manifest
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
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
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ConfigID:    keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					ID:          keboola.RowID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`)),
				},
				Name:        "disabled",
				Description: "test fixture",
				IsDisabled:  true,
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
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ConfigID:    keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					ID:          keboola.RowID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						`foo/extractor/keboola.ex-db-mysql/with-rows`,
						`rows/disabled`,
					),
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ConfigID:    keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					ID:          keboola.RowID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`)),
				},
				Name:        "test_view",
				Description: "test fixture",
				IsDisabled:  false,
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
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ConfigID:    keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					ID:          keboola.RowID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						`foo/extractor/keboola.ex-db-mysql/with-rows`,
						`rows/test-view`,
					),
				},
			},
		},
		{
			Remote: &model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ConfigID:    keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					ID:          keboola.RowID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`)),
				},
				Name:        "users",
				Description: "test fixture",
				IsDisabled:  false,
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
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_FOO_ID`))),
					ComponentID: "keboola.ex-db-mysql",
					ConfigID:    keboola.ConfigID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID`)),
					ID:          keboola.RowID(envs.MustGet(`TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
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
	m := manifest.New(1, "connection.keboola.com")
	m.SetSortBy(model.SortByPath)
	m.SetNamingTemplate(naming.TemplateWithoutIds())
	return m
}

func loadRemoteState(t *testing.T, m *manifest.Manifest, projectStateFile string) (*State, *env.Map, error) {
	t.Helper()

	testProject := testproject.GetTestProjectForTest(t, "")
	fs, err := aferofs.NewLocalFs(t.TempDir())
	require.NoError(t, err)
	switch projectStateFile {
	case "empty.json":
		err := fs.WriteFile(context.Background(), filesystem.NewRawFile(filesystem.Join(fs.WorkingDir(), projectStateFile), `
{
  "allBranchesConfigs": [],
  "branches": [
    {
      "branch": {
        "name": "Main",
        "isDefault": true
      }
    }
  ]
}`))
		require.NoError(t, err)
	case "complex.json":
		err := fs.WriteFile(context.Background(), filesystem.NewRawFile(filesystem.Join(fs.WorkingDir(), projectStateFile), `
{
  "allBranchesConfigs": [
    "empty"
  ],
  "branches": [
    {
      "branch": {
        "name": "Main",
        "description": "Main branch",
        "isDefault": true
      }
    },
    {
      "branch": {
        "name": "Foo",
        "description": "Foo branch",
        "isDefault": false
      },
      "configs": [
        "with-rows"
      ]
    },
    {
      "branch": {
        "name": "Bar",
        "description": "Bar branch",
        "isDefault": false
      },
      "configs": [
        "without-rows"
      ]
    }
  ]
}`))
		require.NoError(t, err)
	}

	err = testProject.SetState(context.Background(), fs, projectStateFile)
	require.NoError(t, err)
	d := dependencies.NewMocked(t, context.Background(), dependencies.WithTestProject(testProject))
	state, err := New(context.Background(), project.NewWithManifest(context.Background(), aferofs.NewMemoryFs(), m), d)
	require.NoError(t, err)
	filter := m.Filter()
	_, localErr, remoteErr := state.Load(context.Background(), LoadOptions{RemoteFilter: filter, LoadRemoteState: true})
	require.NoError(t, localErr)
	return state, testProject.Env(), remoteErr
}
