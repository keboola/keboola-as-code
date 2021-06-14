package fixtures

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/api"
	"keboola-as-code/src/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// TestDumpProjectState dumps test project as JSON file
// Result file is ignored in .gitignore
func TestDumpProjectState(t *testing.T) {
	// Load remote state and convert
	a, _ := api.TestStorageApiWithToken(t)
	remoteState, err := api.LoadState(a)
	assert.NoError(t, err)
	fixtures, err := ConvertRemoteStateToFixtures(remoteState)
	if err != nil {
		assert.FailNow(t, "%s", err)
	}

	// Convert to JSON
	data, err := json.Encode(fixtures, false)
	assert.NoError(t, err)

	// Replace secrets, eg. "#password": "KBC::P..." -> "#password": "my-secret"
	reg := regexp.MustCompile(`(\s*"#[^"]+": ")KBC::[^"]+(")`)
	data = reg.ReplaceAll(data, []byte("${1}my-secret${2}"))

	// Write
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	filePath := filepath.Join(testDir, "project_state.json")
	assert.NoError(t, os.WriteFile(filePath, data, 0666))
	fmt.Printf("Dumped to the file \"%s\"\n", filePath)
}
