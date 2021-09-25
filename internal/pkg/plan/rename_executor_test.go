package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRename(t *testing.T) {
	m, logger, logs := newTestManifest(t)
	fs := m.Fs()

	// Dir structure
	assert.NoError(t, fs.Mkdir(`foo1/sub`))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`foo1/sub/file`), `content`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`foo2`), `content`)))
	logs.Truncate()

	// Plan
	plan := &RenamePlan{
		actions: []*RenameAction{
			{
				OldPath:     "foo1",
				NewPath:     "bar1",
				Description: "foo1 -> bar1",
			},
			{
				OldPath:     "foo2",
				NewPath:     "bar2",
				Description: "foo2 -> bar2",
			},
		},
	}

	// Rename
	executor := newRenameExecutor(logger, m, plan)
	warn, err := executor.invoke()
	assert.Empty(t, warn)
	assert.Empty(t, err)
	assert.True(t, fs.IsFile(`bar1/sub/file`))
	assert.True(t, fs.IsFile(`bar2`))
	assert.False(t, fs.Exists(`foo1/sub/file`))
	assert.False(t, fs.Exists(`foo1`))
	assert.False(t, fs.Exists(`foo2`))

	// Logs
	expectedLog := `
DEBUG  Starting renaming of the 2 paths.
DEBUG  Copied foo1 -> bar1
DEBUG  Copied foo2 -> bar2
DEBUG  Removing old paths.
DEBUG  Removed foo1
DEBUG  Removed foo2
`
	assert.Equal(t, strings.TrimLeft(expectedLog, "\n"), logs.String())
}

func TestRenameFailedKeepOldState(t *testing.T) {
	m, logger, logs := newTestManifest(t)
	fs := m.Fs()

	// Dir structure
	assert.NoError(t, fs.Mkdir(`foo1/sub`))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(`foo1/sub/file`, `content`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(`foo2`, `content`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(`foo5`, `content`)))
	logs.Truncate()

	// Plan
	plan := &RenamePlan{
		actions: []*RenameAction{
			{
				OldPath:     "foo1",
				NewPath:     "bar1",
				Description: "foo1 -> bar1",
			},
			{
				OldPath:     "foo2",
				NewPath:     "bar2",
				Description: "foo2 -> bar2",
			},
			{
				OldPath:     "missing3",
				NewPath:     "missing4",
				Description: "missing3 -> missing4",
			},
			{
				OldPath:     "foo5",
				NewPath:     "bar5",
				Description: "foo5 -> bar5",
			},
		},
	}

	// Rename
	executor := newRenameExecutor(logger, m, plan)
	warn, err := executor.invoke()
	assert.Empty(t, warn)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), `cannot copy "missing3" -> "missing4"`)
	assert.False(t, fs.Exists(`bar1/sub/file`))
	assert.False(t, fs.Exists(`bar1`))
	assert.False(t, fs.Exists(`bar2`))
	assert.False(t, fs.Exists(`bar5`))
	assert.True(t, fs.IsFile(`foo1/sub/file`))
	assert.True(t, fs.IsFile(`foo2`))
	assert.True(t, fs.IsFile(`foo5`))

	// Logs
	expectedLog := `
DEBUG  Starting renaming of the 4 paths.
DEBUG  Copied foo1 -> bar1
DEBUG  Copied foo2 -> bar2
DEBUG  Copied foo5 -> bar5
DEBUG  An error occurred, reverting rename.
DEBUG  Removed bar1
DEBUG  Removed bar2
DEBUG  Removed bar5
INFO  Error occurred, the rename operation was reverted.
`
	assert.Equal(t, strings.TrimLeft(expectedLog, "\n"), logs.String())
}

func TestRenameInvalidOldPath(t *testing.T) {
	dir := t.TempDir()
	plan := &RenamePlan{
		actions: []*RenameAction{
			{
				OldPath:     "relative path",
				NewPath:     filepath.Join(dir, "bar1"),
				Description: "",
			},
		},
	}

	logger, _ := utils.NewDebugLogger()
	executor := newRenameExecutor(logger, dir, &manifest.Manifest{}, plan)
	assert.PanicsWithError(t, "old path must be absolute", func() {
		executor.invoke()
	})
}

func TestRenameInvalidNewPath(t *testing.T) {
	dir := t.TempDir()
	plan := &RenamePlan{
		actions: []*RenameAction{
			{
				OldPath:     filepath.Join(dir, "bar1"),
				NewPath:     "relative path",
				Description: "",
			},
		},
	}

	logger, _ := utils.NewDebugLogger()
	executor := newRenameExecutor(logger, dir, &manifest.Manifest{}, plan)
	assert.PanicsWithError(t, "new path must be absolute", func() {
		executor.invoke()
	})
func newTestManifest(t *testing.T) (*manifest.Manifest, *zap.SugaredLogger, *utils.Writer) {
	t.Helper()
	logger, logs := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	m, err := manifest.NewManifest(1, "foo", fs)
	assert.NoError(t, err)
	return m, logger, logs
}
