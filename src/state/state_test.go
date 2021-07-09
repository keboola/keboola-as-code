package state

import (
	"context"
	"github.com/otiai10/copy"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadStateDifferentProjectId(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	api, _ := remote.TestStorageApi(t)
	api = api.WithToken(&model.Token{Owner: model.TokenOwner{Id: 45678}})
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	m, err := manifest.NewManifest(12345, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	state, ok := LoadState(m, logger, context.Background(), api, true)
	assert.NotNil(t, state)
	assert.False(t, ok)
	assert.Equal(t, "- used token is from the project \"45678\", but it must be from the project \"12345\"", state.LocalErrors().Error())
}

func TestLoadState(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	api, _ := remote.TestStorageApiWithToken(t)
	remote.SetStateOfTestProject(t, api, "minimal.json")

	// Same IDs in local and remote state
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`))
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`))
	projectDir, metadataDir := initLocalState(t, "minimal")
	logger, _ := utils.NewDebugLogger()

	m, err := manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
	m.Project.Id = utils.TestProjectId()
	state, ok := LoadState(m, logger, context.Background(), api, true)
	assert.True(t, ok)
	assert.Empty(t, state.RemoteErrors())
	assert.Empty(t, state.LocalErrors())
	assert.Equal(t, []*BranchState{
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
			BranchManifest: &manifest.BranchManifest{
				RecordState: manifest.RecordState{
					Persisted: true,
				},
				BranchKey: model.BranchKey{
					Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				},
				Paths: manifest.Paths{
					Path:       "main",
					ParentPath: "",
				},
			},
		},
	}, utils.SortByName(state.Branches()))
	assert.Equal(t, []*ConfigState{
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
			ConfigManifest: &manifest.ConfigManifest{
				RecordState: manifest.RecordState{
					Persisted: true,
				},
				ConfigKey: model.ConfigKey{
					BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
					ComponentId: "ex-generic-v2",
					Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				},
				Paths: manifest.Paths{
					Path:       "extractor/ex-generic-v2/456-todos",
					ParentPath: "main",
				},
			},
		},
	}, state.Configs())
	assert.Empty(t, utils.SortByName(state.ConfigRows()))
}

func initLocalState(t *testing.T, localState string) (string, string) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	localStateDir := filepath.Join(testDir, "..", "fixtures", "local", localState)
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	err := copy.Copy(localStateDir, projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	utils.ReplaceEnvsDir(projectDir)
	return projectDir, metadataDir
}
