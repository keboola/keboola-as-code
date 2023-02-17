package primarykey_test

import (
	"os"
	"path/filepath"
	"testing"

	"entgo.io/ent/entc/gen"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/schema/compiler/extension/primarykey"
)

func TestGenerateKeys(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	config := &gen.Config{
		Schema:  "./fixture",
		Target:  targetDir,
		Package: "test/model",
	}

	// Generate
	if !assert.NoError(t, primarykey.GenerateKeys(config)) {
		return
	}

	// Read all files
	files := make(map[string]string)
	assert.NoError(t, filepath.Walk(targetDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			b, err := os.ReadFile(path)
			assert.NoError(t, err)
			relPath, _ := filepath.Rel(targetDir, path)
			files[relPath] = string(b)
		}
		return nil
	}))

	// Assert files content
	assert.Len(t, files, 2)
	assert.Equal(t, expectedChildKeyGo(t), files["key/childKey.go"])
	assert.Equal(t, expectedSubChildKeyGo(t), files["key/subChildKey.go"])
}

func expectedChildKeyGo(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("fixture/expectedChildKeyGo.txt")
	assert.NoError(t, err)
	return string(b)
}

func expectedSubChildKeyGo(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("fixture/expectedSubChildKeyGo.txt")
	assert.NoError(t, err)
	return string(b)
}
