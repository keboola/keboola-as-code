package twinformat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// mockScannerDeps implements ScannerDependencies for testing.
type mockScannerDeps struct {
	fs        filesystem.Fs
	logger    log.Logger
	telemetry telemetry.Telemetry
}

func (m *mockScannerDeps) Fs() filesystem.Fs             { return m.fs }
func (m *mockScannerDeps) Logger() log.Logger            { return m.logger }
func (m *mockScannerDeps) Telemetry() telemetry.Telemetry { return m.telemetry }

func newTestScanner(t *testing.T) (*Scanner, filesystem.Fs) {
	t.Helper()

	fs := aferofs.NewMemoryFs(filesystem.WithLogger(log.NewNopLogger()))
	deps := &mockScannerDeps{
		fs:        fs,
		logger:    log.NewNopLogger(),
		telemetry: telemetry.NewNop(),
	}

	return NewScanner(deps), fs
}

func TestNewScanner(t *testing.T) {
	t.Parallel()

	scanner, _ := newTestScanner(t)

	assert.NotNil(t, scanner)
	assert.NotNil(t, scanner.fs)
	assert.NotNil(t, scanner.logger)
}

func TestScanTransformations_NoTransformationDir(t *testing.T) {
	t.Parallel()

	scanner, _ := newTestScanner(t)
	ctx := context.Background()

	// Project directory exists but no transformation directory
	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	assert.Empty(t, result.Transformations)
	assert.Empty(t, result.Failures)
}

func TestScanTransformations_EmptyTransformationDir(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create empty transformation directory
	require.NoError(t, fs.Mkdir(ctx, "/project/main/transformation"))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	assert.Empty(t, result.Transformations)
	assert.Empty(t, result.Failures)
}

func TestScanTransformations_SingleTransformation(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation directory structure
	configPath := "/project/main/transformation/keboola.snowflake-transformation/my-config"
	require.NoError(t, fs.Mkdir(ctx, configPath))

	// Create config.json
	configJSON := `{
		"storage": {
			"input": {
				"tables": [{"source": "in.c-bucket.source", "destination": "source_table"}]
			},
			"output": {
				"tables": [{"source": "result", "destination": "out.c-bucket.result"}]
			}
		}
	}`
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/config.json", configJSON)))

	// Create meta.json
	metaJSON := `{"name": "My Transformation", "isDisabled": false}`
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", metaJSON)))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	require.Len(t, result.Transformations, 1)
	assert.Equal(t, "keboola.snowflake-transformation", result.Transformations[0].ComponentID)
	assert.Equal(t, "My Transformation", result.Transformations[0].Name)
	assert.Equal(t, "my-config", result.Transformations[0].ConfigID)
	assert.False(t, result.Transformations[0].IsDisabled)
	assert.Len(t, result.Transformations[0].InputTables, 1)
	assert.Equal(t, "in.c-bucket.source", result.Transformations[0].InputTables[0].Source)
	assert.Len(t, result.Transformations[0].OutputTables, 1)
	assert.Equal(t, "out.c-bucket.result", result.Transformations[0].OutputTables[0].Destination)
	assert.Empty(t, result.Failures)
}

func TestScanTransformations_WithDescription(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation directory structure
	configPath := "/project/main/transformation/keboola.python-transformation-v2/python-config"
	require.NoError(t, fs.Mkdir(ctx, configPath))

	// Create meta.json
	metaJSON := `{"name": "Python Transform", "isDisabled": true}`
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", metaJSON)))

	// Create description.md
	description := "This is a test description.\n\nWith multiple lines."
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/description.md", description)))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	require.Len(t, result.Transformations, 1)
	assert.Equal(t, "Python Transform", result.Transformations[0].Name)
	assert.True(t, result.Transformations[0].IsDisabled)
	assert.Equal(t, "This is a test description.\n\nWith multiple lines.", result.Transformations[0].Description)
}

func TestScanTransformations_WithCodeBlocks(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation directory structure
	configPath := "/project/main/transformation/keboola.snowflake-transformation/transform"
	require.NoError(t, fs.Mkdir(ctx, configPath))

	// Create meta.json
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", `{"name": "Transform"}`)))

	// Create blocks directory with code
	blockPath := configPath + "/blocks/001-block/001-code"
	require.NoError(t, fs.Mkdir(ctx, blockPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(blockPath+"/code.sql", "SELECT * FROM source;")))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(blockPath+"/meta.json", `{"name": "Select All"}`)))

	// Block meta
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/blocks/001-block/meta.json", `{"name": "Main Block"}`)))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	require.Len(t, result.Transformations, 1)
	require.Len(t, result.Transformations[0].Blocks, 1)
	assert.Equal(t, "Main Block", result.Transformations[0].Blocks[0].Name)
	require.Len(t, result.Transformations[0].Blocks[0].Codes, 1)
	assert.Equal(t, "Select All", result.Transformations[0].Blocks[0].Codes[0].Name)
	assert.Equal(t, "sql", result.Transformations[0].Blocks[0].Codes[0].Language)
	assert.Equal(t, "SELECT * FROM source;", result.Transformations[0].Blocks[0].Codes[0].Script)
}

func TestScanTransformations_PythonCode(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation directory structure
	configPath := "/project/main/transformation/keboola.python-transformation-v2/python"
	require.NoError(t, fs.Mkdir(ctx, configPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", `{"name": "Python"}`)))

	// Create blocks with Python code
	blockPath := configPath + "/blocks/001-block/001-code"
	require.NoError(t, fs.Mkdir(ctx, blockPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(blockPath+"/code.py", "print('hello')")))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	require.Len(t, result.Transformations, 1)
	require.Len(t, result.Transformations[0].Blocks, 1)
	require.Len(t, result.Transformations[0].Blocks[0].Codes, 1)
	assert.Equal(t, "python", result.Transformations[0].Blocks[0].Codes[0].Language)
	assert.Equal(t, "print('hello')", result.Transformations[0].Blocks[0].Codes[0].Script)
}

func TestScanTransformations_RCode(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation directory structure
	configPath := "/project/main/transformation/keboola.r-transformation/r-transform"
	require.NoError(t, fs.Mkdir(ctx, configPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", `{"name": "R Transform"}`)))

	// Create blocks with R code
	blockPath := configPath + "/blocks/001-block/001-code"
	require.NoError(t, fs.Mkdir(ctx, blockPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(blockPath+"/code.r", "print('hello')")))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	require.Len(t, result.Transformations, 1)
	require.Len(t, result.Transformations[0].Blocks, 1)
	require.Len(t, result.Transformations[0].Blocks[0].Codes, 1)
	assert.Equal(t, "r", result.Transformations[0].Blocks[0].Codes[0].Language)
}

func TestScanTransformations_InvalidConfigJSON(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation directory structure
	configPath := "/project/main/transformation/keboola.snowflake-transformation/invalid"
	require.NoError(t, fs.Mkdir(ctx, configPath))

	// Create invalid config.json
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/config.json", "invalid json")))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	// Should have a failure recorded
	assert.Empty(t, result.Transformations)
	require.Len(t, result.Failures, 1)
	assert.Contains(t, result.Failures[0].Error, "failed to read config.json")
	assert.Contains(t, result.Failures[0].Path, "invalid")
}

func TestScanTransformations_MultipleTransformations(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create multiple transformations across different components
	configs := []struct {
		component string
		configID  string
		name      string
	}{
		{"keboola.snowflake-transformation", "snowflake-1", "Snowflake Transform"},
		{"keboola.snowflake-transformation", "snowflake-2", "Another Snowflake"},
		{"keboola.python-transformation-v2", "python-1", "Python Transform"},
	}

	for _, cfg := range configs {
		path := "/project/main/transformation/" + cfg.component + "/" + cfg.configID
		require.NoError(t, fs.Mkdir(ctx, path))
		require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(path+"/meta.json", `{"name": "`+cfg.name+`"}`)))
	}

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	assert.Len(t, result.Transformations, 3)
	assert.Empty(t, result.Failures)

	// Verify we got all transformations (order may vary)
	names := make(map[string]bool)
	for _, t := range result.Transformations {
		names[t.Name] = true
	}
	assert.True(t, names["Snowflake Transform"])
	assert.True(t, names["Another Snowflake"])
	assert.True(t, names["Python Transform"])
}

func TestScanTransformations_SkipsNonDirectories(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation directory with a file (not directory)
	transformDir := "/project/main/transformation"
	require.NoError(t, fs.Mkdir(ctx, transformDir))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(transformDir+"/some-file.txt", "content")))

	// Create a valid transformation directory
	configPath := transformDir + "/keboola.snowflake-transformation/valid"
	require.NoError(t, fs.Mkdir(ctx, configPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", `{"name": "Valid"}`)))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	// Should only have the valid transformation
	assert.Len(t, result.Transformations, 1)
	assert.Equal(t, "Valid", result.Transformations[0].Name)
}

func TestScanResult_FailuresCollected(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create one valid and one invalid transformation
	validPath := "/project/main/transformation/keboola.snowflake-transformation/valid"
	require.NoError(t, fs.Mkdir(ctx, validPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(validPath+"/meta.json", `{"name": "Valid"}`)))

	invalidPath := "/project/main/transformation/keboola.snowflake-transformation/invalid"
	require.NoError(t, fs.Mkdir(ctx, invalidPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(invalidPath+"/config.json", "not json")))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	// Both valid transformation and failure should be recorded
	assert.Len(t, result.Transformations, 1)
	assert.Len(t, result.Failures, 1)
	assert.Equal(t, "Valid", result.Transformations[0].Name)
	assert.Contains(t, result.Failures[0].Path, "invalid")
}

func TestScanTransformations_NoCodeFilesInBlock(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation with empty code block (no code files)
	configPath := "/project/main/transformation/keboola.snowflake-transformation/transform"
	require.NoError(t, fs.Mkdir(ctx, configPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", `{"name": "Transform"}`)))

	// Create blocks directory with a code subdirectory but no code files
	blockPath := configPath + "/blocks/001-block/001-code"
	require.NoError(t, fs.Mkdir(ctx, blockPath))
	// Don't create any code.sql, code.py, or code.r file

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	require.Len(t, result.Transformations, 1)
	// Block should be empty (no codes) and thus not included
	assert.Empty(t, result.Transformations[0].Blocks)
}

func TestScanTransformations_EmptyStorageConfig(t *testing.T) {
	t.Parallel()

	scanner, fs := newTestScanner(t)
	ctx := context.Background()

	// Create transformation with config.json but no storage section
	configPath := "/project/main/transformation/keboola.snowflake-transformation/transform"
	require.NoError(t, fs.Mkdir(ctx, configPath))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/meta.json", `{"name": "Transform"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(configPath+"/config.json", `{"parameters": {}}`)))

	result, err := scanner.ScanTransformations(ctx, "/project")
	require.NoError(t, err)

	require.Len(t, result.Transformations, 1)
	assert.Empty(t, result.Transformations[0].InputTables)
	assert.Empty(t, result.Transformations[0].OutputTables)
}
