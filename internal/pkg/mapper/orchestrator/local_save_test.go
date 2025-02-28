package orchestrator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestOrchestratorMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Recipe
	orchestratorConfigState := createLocalSaveFixtures(t, state, true)
	recipe := model.NewLocalSaveRecipe(orchestratorConfigState.Manifest(), orchestratorConfigState.Remote, model.NewChangedFields())

	// Save
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Minify JSON + remove file description
	files := make([]filesystem.File, 0, len(recipe.Files.All()))
	for _, file := range recipe.Files.All() {
		var fileRaw *filesystem.RawFile
		if f, ok := file.(*filesystem.JSONFile); ok {
			// Minify JSON
			fileRaw = filesystem.NewRawFile(f.Path(), json.MustEncodeString(f.Content, false))
			fileRaw.AddTag(f.AllTags()...)
		} else {
			var err error
			fileRaw, err = file.ToRawFile()
			require.NoError(t, err)
			fileRaw.SetDescription(``)
		}
		files = append(files, fileRaw)
	}

	// Check generated files
	configDir := orchestratorConfigState.Path()
	phasesDir := state.NamingGenerator().PhasesDir(configDir)
	assert.Equal(t, []filesystem.File{
		filesystem.
			NewRawFile(phasesDir+`/.gitkeep`, ``).
			AddTag(model.FileKindGitKeep).
			AddTag(model.FileTypeOther),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":[],"foo":"bar"}`,
			).
			AddTag(model.FileKindPhaseConfig).
			AddTag(model.FileTypeJSON),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","enabled":true,"task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJSON),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2 - disabled","enabled":false,"task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJSON),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/003-task-3/task.json`,
				`{"name":"Task 3 - disabled without configId","enabled":false,"task":{"mode":"run","componentId":"foo.bar2"},"continueOnFailure":false}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJSON),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/phase.json`,
				`{"name":"Phase With Deps","dependsOn":["001-phase"]}`,
			).
			AddTag(model.FileKindPhaseConfig).
			AddTag(model.FileTypeJSON),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/001-task-4/task.json`,
				`{"name":"Task 4","enabled":true,"task":{"mode":"run","configPath":"extractor/target-config-3"},"continueOnFailure":false}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJSON),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/002-task-5/task.json`,
				`{"name":"Task 5 - configData","enabled":true,"task":{"mode":"run","configData":{"params":"value"},"componentId":"foo.bar3"},"continueOnFailure":false}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(configDir+`/meta.json`, `{"name":"My Orchestration","isDisabled":false}`).
			AddTag(model.FileKindObjectMeta).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(configDir+`/config.json`, `{}`).
			AddTag(model.FileKindObjectConfig).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(configDir+`/description.md`, "\n").
			AddTag(model.FileKindObjectDescription).
			AddTag(model.FileTypeMarkdown),
	}, files)
}

func TestMapBeforeLocalSaveWarnings(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Recipe
	orchestratorConfigState := createLocalSaveFixtures(t, state, false)
	recipe := model.NewLocalSaveRecipe(orchestratorConfigState.Manifest(), orchestratorConfigState.Remote, model.NewChangedFields())

	// Save
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	expectedWarnings := `
WARN  Warning:
- Cannot save orchestrator config "branch/other/orchestrator":
  - Cannot save phase "001-phase":
    - Cannot save task "001-task-1":
      - Config "branch:123/component:foo.bar1/config:123" not found.
    - Cannot save task "002-task-2":
      - Config "branch:123/component:foo.bar2/config:789" not found.
  - Cannot save phase "002-phase-with-deps":
    - Cannot save task "001-task-4":
      - Config "branch:123/component:foo.bar2/config:456" not found.
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.AllMessagesTxt())
}
