package rename

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	validatorPkg "github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestRename(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	validator := validatorPkg.New()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	manifest := projectManifest.New(1, "foo")
	ctx := t.Context()

	// Dir structure
	require.NoError(t, fs.Mkdir(ctx, `foo1/sub`))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`foo1/sub/file`), `content`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`foo2`), `content`)))
	logger.Truncate()

	// Plan
	plan := &Plan{
		actions: []model.RenameAction{
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "foo1",
				NewPath:     "bar1",
				Description: "foo1 -> bar1",
			},
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "foo2",
				NewPath:     "bar2",
				Description: "foo2 -> bar2",
			},
		},
	}

	// NewPlan
	projectState := state.NewRegistry(knownpaths.NewNop(ctx), naming.NewRegistry(), model.NewComponentsMap(nil), model.SortByPath)
	localManager := local.NewManager(logger, validator, fs, fs.FileLoader(), manifest, nil, projectState, mapper.New())
	executor := newRenameExecutor(t.Context(), localManager, plan, Options{})
	require.NoError(t, executor.invoke())
	logsStr := logger.AllMessages()
	assert.NotContains(t, logsStr, `warn`)
	assert.True(t, fs.IsFile(ctx, `bar1/sub/file`))
	assert.True(t, fs.IsFile(ctx, `bar2`))
	assert.False(t, fs.Exists(ctx, `foo1/sub/file`))
	assert.False(t, fs.Exists(ctx, `foo1`))
	assert.False(t, fs.Exists(ctx, `foo2`))

	// Logs
	expectedLog := `
{"level":"debug","message":"Starting renaming of the 2 paths."}
{"level":"debug","message":"Copied \"foo1\" -> \"bar1\""}
{"level":"debug","message":"Copied \"foo2\" -> \"bar2\""}
{"level":"debug","message":"Removing old paths."}
{"level":"debug","message":"Removed \"foo1\""}
{"level":"debug","message":"Removed \"foo2\""}
`
	logger.AssertJSONMessages(t, expectedLog)
}

func TestRenameFailedKeepOldState(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	validator := validatorPkg.New()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	manifest := projectManifest.New(1, "foo")
	ctx := t.Context()

	// Dir structure
	require.NoError(t, fs.Mkdir(ctx, `foo1/sub`))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo1/sub/file`, `content`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo2`, `content`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo5`, `content`)))
	logger.Truncate()

	// Plan
	plan := &Plan{
		actions: []model.RenameAction{
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "foo1",
				NewPath:     "bar1",
				Description: "foo1 -> bar1",
			},
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "foo2",
				NewPath:     "bar2",
				Description: "foo2 -> bar2",
			},
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "missing3",
				NewPath:     "missing4",
				Description: "missing3 -> missing4",
			},
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "foo5",
				NewPath:     "bar5",
				Description: "foo5 -> bar5",
			},
		},
	}

	// NewPlan
	projectState := state.NewRegistry(knownpaths.NewNop(ctx), naming.NewRegistry(), model.NewComponentsMap(nil), model.SortByPath)
	localManager := local.NewManager(logger, validator, fs, fs.FileLoader(), manifest, nil, projectState, mapper.New())
	executor := newRenameExecutor(t.Context(), localManager, plan, Options{})
	err := executor.invoke()
	require.Error(t, err)
	logsStr := logger.AllMessages()
	assert.NotContains(t, logsStr, `warn`)
	assert.Contains(t, err.Error(), `cannot copy "missing3" -> "missing4"`)
	assert.False(t, fs.Exists(ctx, `bar1/sub/file`))
	assert.False(t, fs.Exists(ctx, `bar1`))
	assert.False(t, fs.Exists(ctx, `bar2`))
	assert.False(t, fs.Exists(ctx, `bar5`))
	assert.True(t, fs.IsFile(ctx, `foo1/sub/file`))
	assert.True(t, fs.IsFile(ctx, `foo2`))
	assert.True(t, fs.IsFile(ctx, `foo5`))

	// Logs
	expectedLog := `
{"level":"debug","message":"Starting renaming of the 4 paths."}
{"level":"debug","message":"Copied \"foo1\" -> \"bar1\""}
{"level":"debug","message":"Copied \"foo2\" -> \"bar2\""}
{"level":"debug","message":"Copied \"foo5\" -> \"bar5\""}
{"level":"debug","message":"An error occurred, reverting rename."}
{"level":"debug","message":"Removed \"bar1\""}
{"level":"debug","message":"Removed \"bar2\""}
{"level":"debug","message":"Removed \"bar5\""}
{"level":"info","message":"Error occurred, the rename operation was reverted."}
`
	logger.AssertJSONMessages(t, expectedLog)
}

func TestRenameCleanupOnDestinationExists(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	validator := validatorPkg.New()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	manifest := projectManifest.New(1, "foo")
	ctx := t.Context()

	// Prepare destination that conflicts with rename target
	require.NoError(t, fs.Mkdir(ctx, `src-dir`))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`src-dir/file.txt`, `src`)))
	require.NoError(t, fs.Mkdir(ctx, `dst-dir`))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`dst-dir/file.txt`, `dst`)))
	// Also test file->file conflict
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`src-file.txt`, `src-file`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`dst-file.txt`, `dst-file`)))
	logger.Truncate()

	plan := &Plan{
		actions: []model.RenameAction{
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "src-dir",
				NewPath:     "dst-dir",
				Description: "src-dir -> dst-dir",
			},
			{
				Manifest:    &fixtures.MockedManifest{},
				RenameFrom:  "src-file.txt",
				NewPath:     "dst-file.txt",
				Description: "src-file.txt -> dst-file.txt",
			},
		},
	}

	projectState := state.NewRegistry(knownpaths.NewNop(ctx), naming.NewRegistry(), model.NewComponentsMap(nil), model.SortByPath)
	localManager := local.NewManager(logger, validator, fs, fs.FileLoader(), manifest, nil, projectState, mapper.New())
	executor := newRenameExecutor(t.Context(), localManager, plan, Options{Cleanup: true})
	require.NoError(t, executor.invoke())

	// After cleanup, destination should contain source content
	assert.True(t, fs.IsFile(ctx, `dst-dir/file.txt`))
	file, err := fs.ReadFile(ctx, filesystem.NewFileDef(`dst-dir/file.txt`))
	require.NoError(t, err)
	assert.Equal(t, "src", file.Content)

	file2, err := fs.ReadFile(ctx, filesystem.NewFileDef(`dst-file.txt`))
	require.NoError(t, err)
	assert.Equal(t, "src-file", file2.Content)

	// Old sources removed
	assert.False(t, fs.Exists(ctx, `src-dir`))
	assert.False(t, fs.Exists(ctx, `src-file.txt`))

	// Logs capture: cleanup triggers a normal copy log sequence
	logsStr := logger.AllMessages()
	assert.Contains(t, logsStr, `Copied "src-dir" -> "dst-dir"`)
	assert.Contains(t, logsStr, `Copied "src-file.txt" -> "dst-file.txt"`)
}
