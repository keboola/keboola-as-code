package state_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/reflecthelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestLoadState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	testProject := testproject.GetTestProjectForTest(t, "")
	err := testProject.SetState("minimal.json")
	assert.NoError(t, err)
	envs := testProject.Env()

	// Same IDs in local and remote state
	envs.Set("LOCAL_PROJECT_ID", cast.ToString(testProject.ID()))
	envs.Set("TEST_KBC_STORAGE_API_HOST", testProject.StorageAPIHost())
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", envs.MustGet(`TEST_BRANCH_MAIN_ID`))
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`))

	// Container
	m, fs := loadTestManifest(t, envs, "minimal")
	d := dependencies.NewMocked(t, ctx, dependencies.WithTestProject(testProject))

	// Load
	options := LoadOptions{
		LoadLocalState:  true,
		LoadRemoteState: true,
	}
	state, err := New(context.Background(), project.NewWithManifest(context.Background(), fs, m), d)
	assert.NoError(t, err)
	ok, localErr, remoteErr := state.Load(context.Background(), options)

	// Check errors
	assert.True(t, ok)
	assert.NoError(t, localErr)
	assert.NoError(t, remoteErr)

	// Check results
	assert.Equal(t, []*model.BranchState{
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
			Local: &model.Branch{
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
				Metadata:    make(map[string]string),
			},
			BranchManifest: &model.BranchManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				BranchKey: model.BranchKey{
					ID: keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"",
						"main",
					),
					RelatedPaths: []string{naming.MetaFile, naming.DescriptionFile, "../" + naming.DescriptionFile},
				},
			},
		},
	}, reflecthelper.SortByName(state.Branches()))
	assert.Equal(t, []*model.ConfigState{
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
			Local: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Name:        "todos",
				Description: "todos config",
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
			ConfigManifest: &model.ConfigManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigKey: model.ConfigKey{
					BranchID:    keboola.BranchID(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentID: "ex-generic-v2",
					ID:          keboola.ConfigID(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
						"main",
						"extractor/ex-generic-v2/456-todos",
					),
					RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
				},
			},
		},
	}, state.Configs())
	assert.Empty(t, reflecthelper.SortByName(state.ConfigRows()))
}

func loadTestManifest(t *testing.T, envs *env.Map, localState string) (*projectManifest.Manifest, filesystem.Fs) {
	t.Helper()

	// Prepare temp dir with defined state
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	stateDir := filesystem.Join(testDir, "..", "fixtures", "local", localState)

	// Create Fs
	fs := aferofs.NewMemoryFsFrom(stateDir)
	err := testhelper.ReplaceEnvsDir(context.Background(), fs, `/`, envs)
	require.NoError(t, err)

	// Load manifest
	m, err := projectManifest.Load(context.Background(), log.NewNopLogger(), fs, env.Empty(), false)
	assert.NoError(t, err)

	return m, fs
}
