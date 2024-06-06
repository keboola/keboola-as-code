package primarykey_test

import (
	"os"
	"path/filepath"
	"testing"

	"entgo.io/ent/entc/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, primarykey.GenerateKeys(config))
	// Read all files
	files := make(map[string]string)
	require.NoError(t, filepath.Walk(targetDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			b, err := os.ReadFile(path)
			require.NoError(t, err)
			relPath, _ := filepath.Rel(targetDir, path)
			files[relPath] = string(b)
		}
		return nil
	}))

	// Assert files content
	assert.Len(t, files, 2)
	assert.Equal(t, expectedChildKeyGo(t), files[filepath.Join("key", "childKey.go")])
	assert.Equal(t, expectedSubChildKeyGo(t), files[filepath.Join("key", "subChildKey.go")])
}

func expectedChildKeyGo(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("fixture/expectedChildKeyGo.txt")
	require.NoError(t, err)
	return string(b)
}

func expectedSubChildKeyGo(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("fixture/expectedSubChildKeyGo.txt")
	require.NoError(t, err)
	return string(b)
}
