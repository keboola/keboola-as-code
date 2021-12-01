package rename

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRename(t *testing.T) {
	t.Parallel()
	m, logger, logs := newTestManifest(t)
	fs := m.Fs()

	// Dir structure
	assert.NoError(t, fs.Mkdir(`foo1/sub`))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`foo1/sub/file`), `content`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`foo2`), `content`)))
	logs.Truncate()

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
	state := model.NewState(logger, fs, model.NewComponentsMap(nil), model.SortByPath)
	localManager := local.NewManager(logger, fs, m, state, mapper.New(model.MapperContext{}))
	executor := newRenameExecutor(context.Background(), localManager, plan)
	err := executor.invoke()
	assert.NoError(t, err)
	logsStr := logs.String()
	assert.NotContains(t, logsStr, `WARN`)
	assert.True(t, fs.IsFile(`bar1/sub/file`))
	assert.True(t, fs.IsFile(`bar2`))
	assert.False(t, fs.Exists(`foo1/sub/file`))
	assert.False(t, fs.Exists(`foo1`))
	assert.False(t, fs.Exists(`foo2`))

	// Logs
	expectedLog := `
DEBUG  Starting renaming of the 2 paths.
DEBUG  Copied "foo1" -> "bar1"
DEBUG  Copied "foo2" -> "bar2"
DEBUG  Removing old paths.
DEBUG  Removed "foo1"
DEBUG  Removed "foo2"
`
	assert.Equal(t, strings.TrimLeft(expectedLog, "\n"), logsStr)
}

func TestRenameFailedKeepOldState(t *testing.T) {
	t.Parallel()
	m, logger, logs := newTestManifest(t)
	fs := m.Fs()

	// Dir structure
	assert.NoError(t, fs.Mkdir(`foo1/sub`))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(`foo1/sub/file`, `content`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(`foo2`, `content`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(`foo5`, `content`)))
	logs.Truncate()

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
	state := model.NewState(logger, fs, model.NewComponentsMap(nil), model.SortByPath)
	localManager := local.NewManager(logger, fs, m, state, mapper.New(model.MapperContext{}))
	executor := newRenameExecutor(context.Background(), localManager, plan)
	err := executor.invoke()
	assert.Error(t, err)
	logsStr := logs.String()
	assert.NotContains(t, logsStr, `WARN`)
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
DEBUG  Copied "foo1" -> "bar1"
DEBUG  Copied "foo2" -> "bar2"
DEBUG  Copied "foo5" -> "bar5"
DEBUG  An error occurred, reverting rename.
DEBUG  Removed "bar1"
DEBUG  Removed "bar2"
DEBUG  Removed "bar5"
INFO  Error occurred, the rename operation was reverted.
`
	assert.Equal(t, strings.TrimLeft(expectedLog, "\n"), logsStr)
}

func newTestManifest(t *testing.T) (*manifest.Manifest, *zap.SugaredLogger, *utils.Writer) {
	t.Helper()
	logger, logs := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	m, err := manifest.NewManifest(1, "foo", fs)
	assert.NoError(t, err)
	return m, logger, logs
}
