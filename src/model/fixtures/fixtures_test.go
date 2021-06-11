package fixtures

import (
	"encoding/json"
	"fmt"
	"github.com/sanity-io/litter"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/api"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// TestDumpProjectState dumps test project as Go structure
func TestDumpProjectState(t *testing.T) {
	// Enable to dump actual project state to "project_state_new.json" file
	t.Skip()

	// Load remote state and convert
	a, _ := api.TestStorageApiWithToken(t)
	remoteState, err := api.LoadRemoteState(a)
	assert.NoError(t, err)
	fixtures := ConvertRemoteStateToFixtures(remoteState)

	// Convert to JSON
	data, err := json.MarshalIndent(fixtures, "", "   ")
	assert.NoError(t, err)

	// Replace secrets, eg. "#password": "KBC::P..." -> "#password": "my-secret"
	reg := regexp.MustCompile(`(\s*"#[^"]+": ")KBC::[^"]+(")`)
	data = reg.ReplaceAll(data, []byte("${1}my-secret${2}"))

	// Write
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	filePath := filepath.Join(testDir, "project_state_new.json")
	assert.NoError(t, os.WriteFile(filePath, data, 0666))
	fmt.Printf("Dumped to file \"%s\"\n", filePath)

	// Dump remote state to console
	litter.Config.HomePackage = "fixtures"
	litter.Config.DisablePointerReplacement = true
	litter.Config.StripPackageNames = false
	litter.Config.HidePrivateFields = false
	fmt.Println(litter.Sdump(data))
}
