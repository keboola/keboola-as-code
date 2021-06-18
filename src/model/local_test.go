package model

import (
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadLocalStateNoManifest(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "no-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestLoadLocalStateInvalidManifest(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "invalid-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" is not valid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateEmptyManifest(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "empty-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Regexp(t, "^manifest is not valid:.*", err.Error())
}

func TestLoadLocalStateMinimal(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "minimal")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 1)
	assert.Empty(t, state.UntrackedPaths())
	assert.Equal(t, []string{
		"main",
		"main/ex-generic-v2",
		"main/ex-generic-v2/456-todos",
		"main/ex-generic-v2/456-todos/config.json",
		"main/ex-generic-v2/456-todos/meta.json",
		"main/meta.json",
	}, state.TrackedPaths())
}

func TestLoadLocalStateComplex(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "complex")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())
	assert.Equal(t, complexExpectedBranches(state.ProjectDir()), state.BranchesSlice())
	assert.Equal(t, complexExpectedConfigs(state.ProjectDir()), state.ConfigsSlice())
	assert.Equal(t, []string{
		"123-branch/keboola.ex-db-mysql/untrackedDir",
		"123-branch/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/keboola.ex-generic/456-todos/untracked1",
	}, state.UntrackedPaths())
	assert.Equal(t, []string{
		"123-branch",
		"123-branch/keboola.ex-db-mysql",
		"123-branch/keboola.ex-db-mysql/896-tables",
		"123-branch/keboola.ex-db-mysql/896-tables/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/34-test-view/meta.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json",
		"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/meta.json",
		"123-branch/keboola.ex-generic",
		"123-branch/keboola.ex-generic/456-todos",
		"123-branch/keboola.ex-generic/456-todos/config.json",
		"123-branch/keboola.ex-generic/456-todos/meta.json",
		"123-branch/meta.json",
		"main",
		"main/keboola.ex-generic",
		"main/keboola.ex-generic/456-todos",
		"main/keboola.ex-generic/456-todos/config.json",
		"main/keboola.ex-generic/456-todos/meta.json",
		"main/meta.json",
	}, state.TrackedPaths())
}

func TestLoadLocalStateBranchMissingMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "branch-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `branch metadata JSON file "main/meta.json" not found`, err.Error())
}

func TestLoadLocalStateConfigMissingConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-missing-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config JSON file "123-branch/keboola.ex-generic/456-todos/config.json" not found`, err.Error())
}

func TestLoadLocalStateConfigMissingMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config metadata JSON file "123-branch/keboola.ex-generic/456-todos/meta.json" not found`, err.Error())
}

func TestLoadLocalStateConfigRowMissingConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-row-missing-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json" not found`, err.Error())
}

func TestLoadLocalStateConfigRowMissingMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-row-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row metadata JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json" not found`, err.Error())
}

func TestLoadLocalStateBranchInvalidMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "branch-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `branch metadata JSON file "main/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigInvalidConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-invalid-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config JSON file "123-branch/keboola.ex-generic/456-todos/config.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigInvalidMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config metadata JSON file "123-branch/keboola.ex-generic/456-todos/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigRowInvalidConfigJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-row-invalid-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigRowInvalidMetaJson(t *testing.T) {
	defer utils.ResetEnv(t, os.Environ())
	state, err := loadLocalTestState(t, "config-row-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row metadata JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func loadLocalTestState(t *testing.T, projectDirName string) (*State, *utils.Error) {
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	utils.MustSetEnv("LOCAL_STATE_MY_BRANCH_ID", "123")
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", "456")
	utils.MustSetEnv("LOCAL_STATE_MYSQL_CONFIG_ID", "896")

	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	stateDir := filepath.Join(testDir, "..", "fixtures", "local", projectDirName)
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, MetadataDir)
	err := copy.Copy(stateDir, projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	utils.ReplaceEnvsDir(projectDir)
	state := NewState(projectDir, metadataDir)
	return state, LoadLocalState(state)
}

func complexExpectedBranches(projectDir string) []*BranchState {
	return []*BranchState{
		{
			Id: 111,
			Local: &Branch{
				Id:          111,
				Name:        "Main",
				Description: "Main branch",
				IsDefault:   true,
			},
			Manifest: &ManifestBranch{
				Path: "main",
				Id:   111,
			},
			MetadataFile: projectDir + "/main/meta.json",
		},
		{
			Id: 123,
			Local: &Branch{
				Id:          123,
				Name:        "Branch",
				Description: "My branch",
				IsDefault:   false,
			},
			Manifest: &ManifestBranch{
				Path: "123-branch",
				Id:   123,
			},
			MetadataFile: projectDir + "/123-branch/meta.json",
		},
	}
}

func complexExpectedConfigs(projectDir string) []*ConfigState {
	return []*ConfigState{
		{
			BranchId:    111,
			ComponentId: "keboola.ex-generic",
			Id:          "456",
			Local: &Config{
				BranchId:          111,
				ComponentId:       "keboola.ex-generic",
				Id:                "456",
				Name:              "todos",
				Description:       "todos config",
				ChangeDescription: "",
				Config: map[string]interface{}{
					"parameters": map[string]interface{}{
						"api": map[string]interface{}{
							"baseUrl": "https://jsonplaceholder.typicode.com",
						},
					},
				},
				Rows: []*ConfigRow{},
			},
			Manifest: &ManifestConfig{
				BranchId:    111,
				ComponentId: "keboola.ex-generic",
				Path:        "keboola.ex-generic/456-todos",
				Id:          "456",
				Rows:        []*ManifestConfigRow{},
			},
			MetadataFile: projectDir + "/main/keboola.ex-generic/456-todos/meta.json",
			ConfigFile:   projectDir + "/main/keboola.ex-generic/456-todos/config.json",
		},
		{
			BranchId:    123,
			ComponentId: "keboola.ex-db-mysql",
			Id:          "896",
			Local: &Config{
				BranchId:          123,
				ComponentId:       "keboola.ex-db-mysql",
				Id:                "896",
				Name:              "tables",
				Description:       "tables config",
				ChangeDescription: "",
				Config: map[string]interface{}{
					"parameters": map[string]interface{}{
						"db": map[string]interface{}{
							"host": "mysql.example.com",
						},
					},
				},
				Rows: []*ConfigRow{
					{
						BranchId:          123,
						ComponentId:       "keboola.ex-db-mysql",
						ConfigId:          "896",
						Id:                "56",
						Name:              "disabled",
						Description:       "",
						ChangeDescription: "",
						IsDisabled:        true,
						Config: map[string]interface{}{
							"parameters": map[string]interface{}{
								"incremental": false,
							},
						},
					},
					{
						BranchId:          123,
						ComponentId:       "keboola.ex-db-mysql",
						ConfigId:          "896",
						Id:                "34",
						Name:              "test_view",
						Description:       "row description",
						ChangeDescription: "",
						IsDisabled:        false,
						Config: map[string]interface{}{
							"parameters": map[string]interface{}{
								"incremental": false,
							},
						},
					},
					{
						BranchId:          123,
						ComponentId:       "keboola.ex-db-mysql",
						ConfigId:          "896",
						Id:                "12",
						Name:              "users",
						Description:       "",
						ChangeDescription: "",
						IsDisabled:        false,
						Config: map[string]interface{}{
							"parameters": map[string]interface{}{
								"incremental": false,
							},
						},
					},
				},
			},
			Manifest: &ManifestConfig{
				BranchId:    123,
				ComponentId: "keboola.ex-db-mysql",
				Path:        "keboola.ex-db-mysql/896-tables",
				Id:          "896",
				Rows: []*ManifestConfigRow{
					{
						Path: "12-users",
						Id:   "12",
					},
					{
						Path: "34-test-view",
						Id:   "34",
					},
					{
						Path: "56-disabled",
						Id:   "56",
					},
				},
			},
			MetadataFile: projectDir + "/123-branch/keboola.ex-db-mysql/896-tables/meta.json",
			ConfigFile:   projectDir + "/123-branch/keboola.ex-db-mysql/896-tables/config.json",
		},
		{
			BranchId:    123,
			ComponentId: "keboola.ex-generic",
			Id:          "456",
			Local: &Config{
				BranchId:          123,
				ComponentId:       "keboola.ex-generic",
				Id:                "456",
				Name:              "todos",
				Description:       "todos config",
				ChangeDescription: "",
				Config: map[string]interface{}{
					"parameters": map[string]interface{}{
						"api": map[string]interface{}{
							"baseUrl": "https://jsonplaceholder.typicode.com",
						},
					},
				},
				Rows: []*ConfigRow{},
			},
			Manifest: &ManifestConfig{
				BranchId:    123,
				ComponentId: "keboola.ex-generic",
				Path:        "keboola.ex-generic/456-todos",
				Id:          "456",
				Rows:        []*ManifestConfigRow{},
			},
			MetadataFile: projectDir + "/123-branch/keboola.ex-generic/456-todos/meta.json",
			ConfigFile:   projectDir + "/123-branch/keboola.ex-generic/456-todos/config.json",
		},
	}
}
