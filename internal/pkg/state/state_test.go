package state

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/nhatthm/aferocopy"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/thelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLoadStateDifferentProjectId(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	api, _ := remote.TestStorageApi(t)
	api = api.WithToken(&model.Token{Owner: model.TokenOwner{Id: 45678}})
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	m, err := manifest.NewManifest(12345, "connection.keboola.com", fs)
	assert.NoError(t, err)

	stateOptions := NewOptions(m, api, context.Background(), logger)
	stateOptions.LoadLocalState = true
	stateOptions.LoadRemoteState = true
	state, ok := LoadState(stateOptions)
	assert.NotNil(t, state)
	assert.False(t, ok)
	assert.Equal(t, "used token is from the project \"45678\", but it must be from the project \"12345\"", state.LocalErrors().Error())
}

func TestLoadState(t *testing.T) {
	defer thelper.ResetEnv(t, os.Environ())
	api, _ := remote.TestStorageApiWithToken(t)
	remote.SetStateOfTestProject(t, api, "minimal.json")

	// Same IDs in local and remote state
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`))
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`))

	logger, _ := utils.NewDebugLogger()
	m := loadTestManifest(t, "minimal")
	m.Project.Id = thelper.TestProjectId()

	stateOptions := NewOptions(m, api, context.Background(), logger)
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
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				},
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			Local: &model.Branch{
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
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
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "main",
						ParentPath: "",
					},
					RelatedPaths: []string{model.MetaFile, model.DescriptionFile},
				},
			},
		},
	}, utils.SortByName(state.Branches()))
	assert.Equal(t, []*model.ConfigState{
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
			Local: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
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
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: model.Paths{
					PathInProject: model.PathInProject{
						ObjectPath: "extractor/ex-generic-v2/456-todos",
						ParentPath: "main",
					},
					RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
				},
			},
		},
	}, state.Configs())
	assert.Empty(t, utils.SortByName(state.ConfigRows()))
}

func TestValidateState(t *testing.T) {
	// Create state
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", `123`)
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", `456`)

	logger, _ := utils.NewDebugLogger()
	m := loadTestManifest(t, "minimal")
	m.Project.Id = thelper.TestProjectId()

	api, _ := remote.TestMockedStorageApi(t)
	stateOptions := NewOptions(m, api, context.Background(), logger)
	s := newState(stateOptions)

	// Mocked component response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "keboola.foo",
		"type": "writer",
		"name": "Foo",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/keboola.foo`, getGenericExResponder)

	// Add invalid objects
	branchKey := model.BranchKey{Id: 456}
	branch := &model.Branch{BranchKey: branchKey}
	branchManifest := &model.BranchManifest{BranchKey: branchKey}
	branchManifest.ObjectPath = "branch"
	configKey := model.ConfigKey{BranchId: 456, ComponentId: "keboola.foo", Id: "234"}
	config := &model.Config{ConfigKey: configKey}
	assert.NoError(t, s.manifest.TrackRecord(branchManifest))
	s.SetLocalState(branch, branchManifest)
	_, err = s.SetRemoteState(config)
	assert.NoError(t, err)

	// Validate
	s.validate()
	expectedLocalError := `
branch "456" is not valid:
	- key="name", value="", failed "required" validation
`
	expectedRemoteError := `
config "branch:456/component:keboola.foo/config:234" is not valid:
	- key="name", value="", failed "required" validation
	- key="configuration", value="<nil>", failed "required" validation
`
	assert.Equal(t, strings.TrimSpace(expectedLocalError), s.LocalErrors().Error())
	assert.Equal(t, strings.TrimSpace(expectedRemoteError), s.RemoteErrors().Error())
}

func loadTestManifest(t *testing.T, localState string) *manifest.Manifest {
	t.Helper()

	// Prepare temp dir with defined state
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	localStateDir := filesystem.Join(testDir, "..", "fixtures", "local", localState)
	projectDir := t.TempDir()
	if err := aferocopy.Copy(localStateDir, projectDir); err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	thelper.ReplaceEnvsDir(projectDir, nil)

	// Create fs and load manifest
	fs, err := aferofs.NewLocalFs(zap.NewNop().Sugar(), projectDir, ".")
	assert.NoError(t, err)
	m, err := manifest.LoadManifest(fs)
	assert.NoError(t, err)
	m.Project.Id = 12345
	m.Project.ApiHost = "connection.keboola.com"

	return m
}
