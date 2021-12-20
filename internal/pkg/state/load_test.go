package state_test

import (
	"runtime"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	. "github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestLoadState(t *testing.T) {
	t.Parallel()
	envs := env.Empty()

	testProject := testproject.GetTestProject(t, envs)
	testProject.SetState("minimal.json")

	// Same IDs in local and remote state
	envs.Set("LOCAL_PROJECT_ID", cast.ToString(testProject.Id()))
	envs.Set("TEST_KBC_STORAGE_API_HOST", testProject.StorageApi().Host())
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", envs.MustGet(`TEST_BRANCH_MAIN_ID`))
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`))

	// Dependencies
	m, fs := loadTestManifest(t, envs, "minimal")
	d := testdeps.New()
	d.InitFromTestProject(testProject)
	d.SetFs(fs)
	d.SetProjectManifest(m)
	project, err := d.Project()
	assert.NoError(t, err)

	// Load
	options := Options{
		LoadLocalState:  true,
		LoadRemoteState: true,
	}
	state, ok, localErr, remoteErr := LoadState(project, options, d)

	// Check errors
	assert.True(t, ok)
	assert.Empty(t, localErr)
	assert.Empty(t, remoteErr)

	// Check results
	assert.Equal(t, []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			Local: &model.Branch{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
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
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"",
						"main",
					),
					RelatedPaths: []string{naming.MetaFile, naming.DescriptionFile},
				},
			},
		},
	}, utils.SortByName(state.Branches()))
	assert.Equal(t, []*model.ConfigState{
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
			Local: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
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
			},
			ConfigManifest: &model.ConfigManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigKey: model.ConfigKey{
					BranchId:    model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
					ComponentId: "ex-generic-v2",
					Id:          model.ConfigId(envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"main",
						"extractor/ex-generic-v2/456-todos",
					),
					RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
				},
			},
		},
	}, state.Configs())
	assert.Empty(t, utils.SortByName(state.ConfigRows()))
}

func loadTestManifest(t *testing.T, envs *env.Map, localState string) (*projectManifest.Manifest, filesystem.Fs) {
	t.Helper()

	// Prepare temp dir with defined state
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	stateDir := filesystem.Join(testDir, "..", "fixtures", "local", localState)

	// Create Fs
	fs := testfs.NewMemoryFsFrom(stateDir)
	testhelper.ReplaceEnvsDir(fs, `/`, envs)

	// Load manifest
	m, err := projectManifest.Load(fs)
	assert.NoError(t, err)

	return m, fs
}
