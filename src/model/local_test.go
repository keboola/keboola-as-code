package model

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadLocalStateNoManifest(t *testing.T) {
	state, _, err := loadLocalTestState("no-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestLoadLocalStateInvalidManifest(t *testing.T) {
	state, _, err := loadLocalTestState("invalid-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" is not valid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateEmptyManifest(t *testing.T) {
	state, _, err := loadLocalTestState("empty-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Regexp(t, "^manifest is not valid:.*", err.Error())
}

func TestLoadLocalStateMinimal(t *testing.T) {
	state, paths, err := loadLocalTestState("minimal")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())
	assert.Len(t, state.Branches(), 0)
	assert.Len(t, state.Configs(), 0)
	assert.Empty(t, paths.Untracked())
	assert.Empty(t, paths.Tracked())
}

func TestLoadLocalStateComplex(t *testing.T) {
	state, paths, err := loadLocalTestState("complex")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())
	assert.Equal(t, complexExpectedBranches(), state.Branches())
	assert.Equal(t, complexExpectedConfigs(), state.Configs())
	assert.Equal(t, []string{
		"123-branch/keboola.ex-db-mysql/untrackedDir",
		"123-branch/keboola.ex-db-mysql/untrackedDir/untracked2",
		"123-branch/keboola.ex-generic/456-todos/untracked1",
	}, paths.Untracked())
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
	}, paths.Tracked())
}

func TestLoadLocalStateBranchMissingMetaJson(t *testing.T) {
	state, _, err := loadLocalTestState("branch-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `branch metadata JSON file "main/meta.json" not found`, err.Error())
}

func TestLoadLocalStateConfigMissingConfigJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-missing-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config JSON file "123-branch/keboola.ex-generic/456-todos/config.json" not found`, err.Error())
}

func TestLoadLocalStateConfigMissingMetaJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config metadata JSON file "123-branch/keboola.ex-generic/456-todos/meta.json" not found`, err.Error())
}

func TestLoadLocalStateConfigRowMissingConfigJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-row-missing-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json" not found`, err.Error())
}

func TestLoadLocalStateConfigRowMissingMetaJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-row-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row metadata JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json" not found`, err.Error())
}

func TestLoadLocalStateBranchInvalidMetaJson(t *testing.T) {
	state, _, err := loadLocalTestState("branch-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `branch metadata JSON file "main/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigInvalidConfigJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-invalid-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config JSON file "123-branch/keboola.ex-generic/456-todos/config.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigInvalidMetaJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config metadata JSON file "123-branch/keboola.ex-generic/456-todos/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigRowInvalidConfigJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-row-invalid-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadLocalStateConfigRowInvalidMetaJson(t *testing.T) {
	state, _, err := loadLocalTestState("config-row-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row metadata JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func loadLocalTestState(projectDirName string) (*State, *PathsState, *utils.Error) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := filepath.Join(testDir, "fixtures", "local", projectDirName)
	metadataDir := filepath.Join(projectDir, MetadataDir)
	return LoadLocalState(projectDir, metadataDir)
}

func complexExpectedBranches() map[int]*Branch {
	return map[int]*Branch{
		111: {
			Id:          111,
			Name:        "Main",
			Description: "Main branch",
			IsDefault:   true,
		},
		123: {
			Id:          123,
			Name:        "Branch",
			Description: "My branch",
			IsDefault:   false,
		},
	}
}

func complexExpectedConfigs() map[string]*Config {
	return map[string]*Config{
		"111_keboola.ex-generic_456": {
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
		},
		"123_keboola.ex-db-mysql_896": {
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
		"123_keboola.ex-generic_456": {
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
		},
	}
}
