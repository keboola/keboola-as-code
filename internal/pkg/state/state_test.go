package state

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLoadState(t *testing.T) {
	t.Parallel()
	envs := env.Empty()

	project := testproject.GetTestProject(t, envs)
	project.SetState("minimal.json")

	// Same IDs in local and remote state
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", envs.MustGet(`TEST_BRANCH_MAIN_ID`))
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`))

	logger, _ := utils.NewDebugLogger()
	m := loadTestManifest(t, envs, "minimal")
	m.Project.Id = project.Id()

	stateOptions := NewOptions(m, project.StorageApi(), project.SchedulerApi(), context.Background(), logger)
	stateOptions.LoadLocalState = true
	stateOptions.LoadRemoteState = true
	state, ok := LoadState(stateOptions)
	assert.True(t, ok)
	assert.Empty(t, state.RemoteErrors().Errors)
	assert.Empty(t, state.LocalErrors().Errors)
	assert.Equal(t, []*model.BranchState{
		{
			Remote: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			Local: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
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
					Id: cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"",
						"main",
					),
					RelatedPaths: []string{model.MetaFile, model.DescriptionFile},
				},
			},
		},
	}, utils.SortByName(state.Branches()))
	assert.Equal(t, []*model.ConfigState{
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
			Local: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
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
			ConfigManifest: &model.ConfigManifest{
				RecordState: model.RecordState{
					Persisted: true,
				},
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(envs.MustGet(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          envs.MustGet(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.NewPathInProject(
						"main",
						"extractor/ex-generic-v2/456-todos",
					),
					RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
				},
			},
		},
	}, state.Configs())
	assert.Empty(t, utils.SortByName(state.ConfigRows()))
}

func TestValidateState(t *testing.T) {
	t.Parallel()
	// Create state
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", `123`)
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", `456`)

	logger, _ := utils.NewDebugLogger()
	m := loadTestManifest(t, envs, "minimal")
	m.Project.Id = 123

	api, httpTransport, _ := testapi.NewMockedStorageApi()

	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	stateOptions := NewOptions(m, api, schedulerApi, context.Background(), logger)
	s := newState(stateOptions)

	// Mocked component response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "keboola.foo",
		"type": "writer",
		"name": "Foo",
	})
	assert.NoError(t, err)
	httpTransport.RegisterResponder("GET", `=~/storage/components/keboola.foo`, getGenericExResponder)

	// Add invalid objects
	branchKey := model.BranchKey{Id: 456}
	branch := &model.Branch{BranchKey: branchKey}
	branchManifest := &model.BranchManifest{BranchKey: branchKey}
	branchManifest.ObjectPath = "branch"
	configKey := model.ConfigKey{BranchId: 456, ComponentId: "keboola.foo", Id: "234"}
	config := &model.Config{ConfigKey: configKey}
	configManifest := &model.ConfigManifest{ConfigKey: configKey}
	assert.NoError(t, s.manifest.PersistRecord(branchManifest))
	branchState, err := s.CreateFrom(branchManifest)
	assert.NoError(t, err)
	branchState.SetLocalState(branch)
	configState, err := s.CreateFrom(configManifest)
	assert.NoError(t, err)
	configState.SetRemoteState(config)
	assert.NoError(t, err)

	// Validate
	s.validate()
	expectedLocalError := `
local branch "branch" is not valid:
  - key="name", value="", failed "required" validation
`
	expectedRemoteError := `
remote config "branch:456/component:keboola.foo/config:234" is not valid:
  - key="name", value="", failed "required" validation
  - key="configuration", value="<nil>", failed "required" validation
`
	assert.Equal(t, strings.TrimSpace(expectedLocalError), s.LocalErrors().Error())
	assert.Equal(t, strings.TrimSpace(expectedRemoteError), s.RemoteErrors().Error())
}

func loadTestManifest(t *testing.T, envs *env.Map, localState string) *manifest.Manifest {
	t.Helper()

	// Prepare temp dir with defined state
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	stateDir := filesystem.Join(testDir, "..", "fixtures", "local", localState)

	// Create Fs
	fs := testhelper.NewMemoryFsFrom(stateDir)
	testhelper.ReplaceEnvsDir(fs, `/`, envs)

	// Load manifest
	m, err := manifest.Load(fs, zap.NewNop().Sugar())
	assert.NoError(t, err)
	m.Project.Id = 12345
	m.Project.ApiHost = "connection.keboola.com"

	return m
}
