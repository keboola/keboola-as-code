package model

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadStateNoManifest(t *testing.T) {
	state, err := loadLocalTestState("no-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" not found`, err.Error())
}

func TestLoadStateInvalidManifest(t *testing.T) {
	state, err := loadLocalTestState("invalid-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `manifest ".keboola/manifest.json" is not valid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadStateEmptyManifest(t *testing.T) {
	state, err := loadLocalTestState("empty-manifest")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Regexp(t, "^manifest is not valid:.*", err.Error())
}

func TestLoadStateMinimal(t *testing.T) {
	state, err := loadLocalTestState("minimal")
	assert.NotNil(t, state)
	assert.Nil(t, err)
	assert.Len(t, state.Branches(), 0)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadStateComplex(t *testing.T) {
	state, err := loadLocalTestState("complex")
	assert.NotNil(t, state)
	assert.Nil(t, err)
	assert.Equal(t, complexExpectedBranches(), state.Branches())
	assert.Equal(t, complexExpectedConfigs(), state.Configs())
}

func TestLoadStateBranchMissingMetaJson(t *testing.T) {
	state, err := loadLocalTestState("branch-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `branch metadata JSON file "main/meta.json" not found`, err.Error())
}

func TestLoadStateConfigMissingConfigJson(t *testing.T) {
	state, err := loadLocalTestState("config-missing-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config JSON file "123-branch/keboola.ex-generic/456-todos/config.json" not found`, err.Error())
}

func TestLoadStateConfigMissingMetaJson(t *testing.T) {
	state, err := loadLocalTestState("config-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config metadata JSON file "123-branch/keboola.ex-generic/456-todos/meta.json" not found`, err.Error())
}

func TestLoadStateConfigRowMissingConfigJson(t *testing.T) {
	state, err := loadLocalTestState("config-row-missing-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json" not found`, err.Error())
}

func TestLoadStateConfigRowMissingMetaJson(t *testing.T) {
	state, err := loadLocalTestState("config-row-missing-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row metadata JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json" not found`, err.Error())
}

func TestLoadStateBranchInvalidMetaJson(t *testing.T) {
	state, err := loadLocalTestState("branch-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `branch metadata JSON file "main/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadStateConfigInvalidConfigJson(t *testing.T) {
	state, err := loadLocalTestState("config-invalid-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config JSON file "123-branch/keboola.ex-generic/456-todos/config.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadStateConfigInvalidMetaJson(t *testing.T) {
	state, err := loadLocalTestState("config-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config metadata JSON file "123-branch/keboola.ex-generic/456-todos/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadStateConfigRowInvalidConfigJson(t *testing.T) {
	state, err := loadLocalTestState("config-row-invalid-config-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func TestLoadStateConfigRowInvalidMetaJson(t *testing.T) {
	state, err := loadLocalTestState("config-row-invalid-meta-json")
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, `config row metadata JSON file "123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json" is invalid: invalid character 'f' looking for beginning of object key string, offset: 3`, err.Error())
}

func loadLocalTestState(projectDirName string) (*State, error) {
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
