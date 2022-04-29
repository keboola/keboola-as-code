package state_test

import (
	"runtime"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	. "github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
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

	// Container
	m, fs := loadTestManifest(t, envs, "minimal")
	d := dependencies.NewTestContainer()
	d.InitFromTestProject(testProject)
	d.SetFs(fs)
	d.SetLocalProject(project.NewWithManifest(d.Fs(), m, d))
	prj, err := d.LocalProject(false)
	assert.NoError(t, err)

	// Load
	options := LoadOptions{
		LoadLocalState:  true,
		LoadRemoteState: true,
	}
	state, err := New(prj, d)
	assert.NoError(t, err)
	ok, localErr, remoteErr := state.Load(options)

	// Check errors
	assert.True(t, ok)
	assert.NoError(t, localErr)
	assert.NoError(t, remoteErr)

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
				Metadata:    make(map[string]string),
			},
			Local: &model.Branch{
				BranchKey: model.BranchKey{
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
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
					Id: model.BranchId(cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`))),
				},
				Paths: model.Paths{
					AbsPath: model.NewAbsPath(
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
				Metadata:          make(map[string]string),
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
				Metadata: make(map[string]string),
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
					AbsPath: model.NewAbsPath(
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
	m, err := projectManifest.Load(fs, false)
	assert.NoError(t, err)

	return m, fs
}
