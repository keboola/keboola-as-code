package api

import (
	"context"
	"fmt"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/fixtures"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestLoadRemoteStateEmpty(t *testing.T) {
	setTestProjectState(t, "empty.json")

	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	a, _ := TestStorageApiWithToken(t)
	state := model.NewState(projectDir, metadataDir)
	err := a.LoadRemoteState(state, context.Background())
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadRemoteStateComplex(t *testing.T) {
	setTestProjectState(t, "complex.json")

	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	a, _ := TestStorageApiWithToken(t)
	state := model.NewState(projectDir, metadataDir)
	err := a.LoadRemoteState(state, context.Background())
	assert.NotNil(t, state)
	assert.NotNil(t, err)
	assert.Equal(t, 0, err.Len())

	// Assert branches
	var remoteBranches []*model.Branch
	for _, branch := range state.Branches() {
		remoteBranches = append(remoteBranches, branch.Remote)
	}
	sort.SliceStable(remoteBranches, func(i, j int) bool {
		return remoteBranches[i].Id < remoteBranches[j].Id
	})
	branchesJson, jsonErr := json.EncodeString(remoteBranches, true)
	assert.NoError(t, jsonErr)
	assert.Equal(t, complexExpectedBranches(), branchesJson)

	// Assert configs
	var remoteConfigs []*model.Config
	for _, config := range state.Configs() {
		remoteConfigs = append(remoteConfigs, config.Remote)
	}
	sort.SliceStable(remoteConfigs, func(i, j int) bool {
		// Sort by: branchId,componentId,name
		switch strings.Compare(cast.ToString(remoteConfigs[i].BranchId), cast.ToString(remoteConfigs[j].BranchId)) {
		case -1:
			return true
		case 1:
			return false
		}

		switch strings.Compare(remoteConfigs[i].ComponentId, remoteConfigs[j].ComponentId) {
		case -1:
			return true
		case 1:
			return false
		}

		return remoteConfigs[i].Name < remoteConfigs[j].Name
	})
	configsJson, jsonErr := json.EncodeString(remoteConfigs, true)
	assert.NoError(t, jsonErr)
	assert.Equal(t, complexExpectedConfigs(), configsJson)
}

// TestDumpProjectState dumps test project as JSON file
// Result file is ignored in .gitignore
func TestDumpProjectState(t *testing.T) {
	// Load remote state and convert
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	a, _ := TestStorageApiWithToken(t)
	state := model.NewState(projectDir, metadataDir)
	stateErr := a.LoadRemoteState(state, context.Background())
	assert.NotNil(t, stateErr)
	if stateErr.Len() > 0 {
		assert.FailNow(t, "%s", stateErr)
	}

	s, err := fixtures.ConvertRemoteStateToFixtures(state)
	if err != nil {
		assert.FailNow(t, "%s", err)
	}

	// Convert to JSON
	data, err := json.Encode(s, true)
	assert.NoError(t, err)

	// Replace secrets, eg. "#password": "KBC::P..." -> "#password": "my-secret"
	reg := regexp.MustCompile(`(\s*"#[^"]+": ")KBC::[^"]+(")`)
	data = reg.ReplaceAll(data, []byte("${1}my-secret${2}"))

	// Write
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	filePath := filepath.Join(testDir, "fixtures", "current_project_state.json")
	assert.NoError(t, os.WriteFile(filePath, data, 0666))
	fmt.Printf("Dumped to the file \"%s\"\n", filePath)
}

func complexExpectedBranches() string {
	return utils.ReplaceEnvsString(`[
  {
    "id": %%TEST_BRANCH_MAIN_ID%%,
    "name": "Main",
    "description": "Main branch",
    "isDefault": true
  },
  {
    "id": %%TEST_BRANCH_FOO_ID%%,
    "name": "Foo",
    "description": "Foo branch",
    "isDefault": false
  },
  {
    "id": %%TEST_BRANCH_BAR_ID%%,
    "name": "Bar",
    "description": "Bar branch",
    "isDefault": false
  }
]`)
}

func complexExpectedConfigs() string {
	return utils.ReplaceEnvsString(`[
  {
    "branchId": %%TEST_BRANCH_MAIN_ID%%,
    "componentId": "ex-generic-v2",
    "id": "%%TEST_BRANCH_ALL_CONFIG_EMPTY_ID%%",
    "name": "empty",
    "description": "test fixture",
    "changeDescription": "created by test",
    "configuration": {},
    "rows": []
  },
  {
    "branchId": %%TEST_BRANCH_FOO_ID%%,
    "componentId": "ex-generic-v2",
    "id": "%%TEST_BRANCH_ALL_CONFIG_EMPTY_ID%%",
    "name": "empty",
    "description": "test fixture",
    "changeDescription": "Copied from default branch configuration \"empty\" (%%TEST_BRANCH_ALL_CONFIG_EMPTY_ID%%) version 1",
    "configuration": {},
    "rows": []
  },
  {
    "branchId": %%TEST_BRANCH_FOO_ID%%,
    "componentId": "keboola.ex-db-mysql",
    "id": "%%TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID%%",
    "name": "with-rows",
    "description": "test fixture",
    "changeDescription": "created by test",
    "configuration": {
      "parameters": {
        "db": {
          "host": "mysql.example.com"
        }
      }
    },
    "rows": [
      {
        "branchId": %%TEST_BRANCH_FOO_ID%%,
        "componentId": "keboola.ex-db-mysql",
        "configId": "%%TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID%%",
        "id": "%%TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_DISABLED_ID%%",
        "name": "disabled",
        "description": "test fixture",
        "changeDescription": "created by test",
        "isDisabled": true,
        "configuration": {
          "parameters": {
            "incremental": false
          }
        }
      },
      {
        "branchId": %%TEST_BRANCH_FOO_ID%%,
        "componentId": "keboola.ex-db-mysql",
        "configId": "%%TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID%%",
        "id": "%%TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_TEST_VIEW_ID%%",
        "name": "test_view",
        "description": "test fixture",
        "changeDescription": "created by test",
        "isDisabled": false,
        "configuration": {
          "parameters": {
            "incremental": false
          }
        }
      },
      {
        "branchId": %%TEST_BRANCH_FOO_ID%%,
        "componentId": "keboola.ex-db-mysql",
        "configId": "%%TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ID%%",
        "id": "%%TEST_BRANCH_FOO_CONFIG_WITH_ROWS_ROW_USERS_ID%%",
        "name": "users",
        "description": "test fixture",
        "changeDescription": "created by test",
        "isDisabled": false,
        "configuration": {
          "parameters": {
            "incremental": false
          }
        }
      }
    ]
  },
  {
    "branchId": %%TEST_BRANCH_BAR_ID%%,
    "componentId": "ex-generic-v2",
    "id": "%%TEST_BRANCH_ALL_CONFIG_EMPTY_ID%%",
    "name": "empty",
    "description": "test fixture",
    "changeDescription": "Copied from default branch configuration \"empty\" (%%TEST_BRANCH_ALL_CONFIG_EMPTY_ID%%) version 1",
    "configuration": {},
    "rows": []
  },
  {
    "branchId": %%TEST_BRANCH_BAR_ID%%,
    "componentId": "ex-generic-v2",
    "id": "%%TEST_BRANCH_BAR_CONFIG_WITHOUT_ROWS_ID%%",
    "name": "without-rows",
    "description": "test fixture",
    "changeDescription": "created by test",
    "configuration": {
      "parameters": {
        "api": {
          "baseUrl": "https://jsonplaceholder.typicode.com"
        }
      }
    },
    "rows": []
  }
]`)
}

func setTestProjectState(t *testing.T, stateFile string) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	SetStateOfTestProject(t, filepath.Join(testDir, "fixtures", "state", stateFile))
}
