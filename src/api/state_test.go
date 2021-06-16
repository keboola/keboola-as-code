package api

import (
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
	"testing"
)

func TestLoadRemoteStateEmpty(t *testing.T) {
	setTestProjectState(t, "empty.json")
	a, _ := TestStorageApiWithToken(t)
	state, err := LoadRemoteState(a)
	assert.NotNil(t, state)
	assert.Nil(t, err)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 0)
}

func TestLoadRemoteStateComplex(t *testing.T) {
	setTestProjectState(t, "complex.json")
	a, _ := TestStorageApiWithToken(t)
	state, err := LoadRemoteState(a)
	assert.NotNil(t, state)
	assert.Nil(t, err)
	assert.Equal(t, complexExpectedBranches(t), state.Branches())

	stateJson, jsonErr := json.EncodeString(state.Configs(), true)
	assert.NoError(t, jsonErr)
	utils.AssertWildcards(t, complexExpectedConfigs(), stateJson, "unexpected state")
}

// TestDumpProjectState dumps test project as JSON file
// Result file is ignored in .gitignore
func TestDumpProjectState(t *testing.T) {
	// Load remote state and convert
	a, _ := TestStorageApiWithToken(t)
	state, stateErr := LoadRemoteState(a)
	if stateErr != nil {
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

func complexExpectedBranches(t *testing.T) map[int]*model.Branch {
	mainBranchId := cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_MAIN_ID`))
	fooBranchId := cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_FOO_ID`))
	barBranchId := cast.ToInt(utils.MustGetEnv(`TEST_BRANCH_BAR_ID`))
	return map[int]*model.Branch{
		mainBranchId: {
			Id:          mainBranchId,
			Name:        "Main",
			Description: "Main branch",
			IsDefault:   true,
		},
		fooBranchId: {
			Id:          fooBranchId,
			Name:        "Foo",
			Description: "Foo branch",
			IsDefault:   false,
		},
		barBranchId: {
			Id:          barBranchId,
			Name:        "Bar",
			Description: "Bar branch",
			IsDefault:   false,
		},
	}
}

func complexExpectedConfigs() string {
	return `{
  "%s": {
    "branchId": %s,
    "componentKey": "ex-generic-v2",
    "id": "%s",
    "name": "empty",
    "description": "test fixture",
    "changeDescription": "created by test",
    "configuration": {},
    "rows": []
  },
  "%s": {
    "branchId": %s,
    "componentKey": "ex-generic-v2",
    "id": "%s",
    "name": "empty",
    "description": "test fixture",
    "changeDescription": "Copied from default branch configuration \"empty\" (%s) version 1",
    "configuration": {},
    "rows": []
  },
  "%s": {
    "branchId": %s,
    "componentKey": "keboola.ex-db-mysql",
    "id": "%s",
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
        "branchId": %s,
        "componentKey": "keboola.ex-db-mysql",
        "configKey": "%s",
        "id": "%s",
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
        "branchId": %s,
        "componentKey": "keboola.ex-db-mysql",
        "configKey": "%s",
        "id": "%s",
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
        "branchId": %s,
        "componentKey": "keboola.ex-db-mysql",
        "configKey": "%s",
        "id": "%s",
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
  "%s": {
    "branchId": %s,
    "componentKey": "ex-generic-v2",
    "id": "%s",
    "name": "empty",
    "description": "test fixture",
    "changeDescription": "Copied from default branch configuration \"empty\" (%s) version 1",
    "configuration": {},
    "rows": []
  },
  "%s": {
    "branchId": %s,
    "componentKey": "ex-generic-v2",
    "id": "%s",
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
}`
}

func setTestProjectState(t *testing.T, stateFile string) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	SetStateOfTestProject(t, filepath.Join(testDir, "fixtures", "state", stateFile))
}
