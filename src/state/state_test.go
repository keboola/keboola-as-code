package state

import (
	"context"
	"github.com/iancoleman/orderedmap"
	"github.com/otiai10/copy"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadState(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	remote.SetStateOfTestProject(t, "minimal.json")

	// Same IDs in local and remote state
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`))
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`))
	projectDir, metadataDir := initLocalState(t, "minimal")
	api, _ := remote.TestStorageApiWithToken(t)
	logger, _ := utils.NewDebugLogger()

	manifest, err := model.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
	state, err := LoadState(manifest, logger, context.Background(), api)
	assert.NoError(t, err)
	assert.Empty(t, state.RemoteErrors())
	assert.Empty(t, state.LocalErrors())
	assert.Equal(t, []*model.BranchState{
		{
			Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
			Remote: &model.Branch{
				Id:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			Local: &model.Branch{
				Id:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			BranchManifest: &model.BranchManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "main",
					ParentPath: "",
				},
				Id: cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
			},
		},
	}, state.Branches())
	assert.Equal(t, []*model.ConfigState{
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
			Local: &model.Config{
				BranchId:          cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				ComponentId:       "ex-generic-v2",
				Id:                utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				Name:              "todos",
				Description:       "todos config",
				ChangeDescription: "",
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
			ConfigManifest: &model.ConfigManifest{
				ManifestPaths: model.ManifestPaths{
					Path:       "ex-generic-v2/456-todos",
					ParentPath: "main",
				},
				BranchId:    cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`)),
				ComponentId: "ex-generic-v2",
				Id:          utils.MustGetEnv(`TEST_BRANCH_ALL_CONFIG_EMPTY_ID`),
				Rows:        []*model.ConfigRowManifest{},
			},
		},
	}, state.Configs())
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
