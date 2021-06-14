package local

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/model"
	"keboola-as-code/src/options"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadStateNoManifest(t *testing.T) {
	state, err := loadState("no-manifest")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Regexp(t, `^manifest file not found ".*/no-manifest/.keboola/manifest.json"`, err.Error())
}

func TestLoadStateInvalidManifest(t *testing.T) {
	state, err := loadState("invalid-manifest")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "manifest is not valid:\n- invalid character 'f' looking for beginning of object key string, offset: 3", err.Error())
}

func TestLoadStateEmptyManifest(t *testing.T) {
	state, err := loadState("empty-manifest")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Regexp(t, "^manifest is not valid:\n.*", err.Error())
}

func TestLoadStateMinimal(t *testing.T) {
	state, err := loadState("minimal")
	assert.NotNil(t, state)
	assert.Nil(t, err)
	assert.Len(t, state.Branches(), 0)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadStateComplex(t *testing.T) {
	state, err := loadState("complex")
	assert.NotNil(t, state)
	assert.Nil(t, err)
	assert.Equal(t, complexExpectedBranches(), state.Branches())
	assert.Equal(t, complexExpectedConfigs(), state.Configs())
}

func TestLoadStateBranchMissingMetaJson(t *testing.T) {
	state, err := loadState("branch-missing-meta-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "branch metadata file not found \"main/meta.json\"", err.Error())
}

func TestLoadStateConfigMissingConfigJson(t *testing.T) {
	state, err := loadState("config-missing-config-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config content file not found \"123-branch/keboola.ex-generic/456-todos/config.json\"", err.Error())
}

func TestLoadStateConfigMissingMetaJson(t *testing.T) {
	state, err := loadState("config-missing-meta-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config metadata file not found \"123-branch/keboola.ex-generic/456-todos/meta.json\"", err.Error())
}

func TestLoadStateConfigRowMissingConfigJson(t *testing.T) {
	state, err := loadState("config-row-missing-config-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config row content file not found \"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/config.json\"", err.Error())
}

func TestLoadStateConfigRowMissingMetaJson(t *testing.T) {
	state, err := loadState("config-row-missing-meta-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config row metadata file not found \"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json\"", err.Error())
}

func TestLoadStateBranchInvalidMetaJson(t *testing.T) {
	state, err := loadState("branch-invalid-meta-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "branch metadata file is invalid, invalid JSON file \"main/meta.json\":\n- invalid character 'f' looking for beginning of object key string, offset: 3", err.Error())
}

func TestLoadStateConfigInvalidConfigJson(t *testing.T) {
	state, err := loadState("config-invalid-config-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config content  is invalid, invalid JSON file \"123-branch/keboola.ex-generic/456-todos/config.json\":\n- invalid character 'f' looking for beginning of object key string, offset: 3", err.Error())
}

func TestLoadStateConfigInvalidMetaJson(t *testing.T) {
	state, err := loadState("config-invalid-meta-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config metadata file is invalid, invalid JSON file \"123-branch/keboola.ex-generic/456-todos/meta.json\":\n- invalid character 'f' looking for beginning of object key string, offset: 3", err.Error())
}

func TestLoadStateConfigRowInvalidConfigJson(t *testing.T) {
	state, err := loadState("config-row-invalid-config-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config row content is invalid, invalid JSON file \"123-branch/keboola.ex-db-mysql/896-tables/rows/56-disabled/config.json\":\n- invalid character 'f' looking for beginning of object key string, offset: 3", err.Error())
}

func TestLoadStateConfigRowInvalidMetaJson(t *testing.T) {
	state, err := loadState("config-row-invalid-meta-json")
	assert.Nil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, "config row metadata file is invalid, invalid JSON file \"123-branch/keboola.ex-db-mysql/896-tables/rows/12-users/meta.json\":\n- invalid character 'f' looking for beginning of object key string, offset: 3", err.Error())
}

func loadState(projectDirName string) (*model.State, error) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := filepath.Join(testDir, "fixtures", projectDirName)
	metadataDir := filepath.Join(projectDir, options.MetadataDir)
	return LoadState(projectDir, metadataDir)
}

func complexExpectedBranches() map[int]*model.Branch {
	return map[int]*model.Branch{
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

func complexExpectedConfigs() map[string]*model.Config {
	return map[string]*model.Config{
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
			Rows: []*model.ConfigRow{
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
