package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
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
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Minify JSON + remove file description
	var files []filesystem.File
	for _, file := range recipe.Files.All() {
		var fileRaw *filesystem.RawFile
		if f, ok := file.(*filesystem.JsonFile); ok {
			// Minify JSON
			fileRaw = filesystem.NewRawFile(f.Path(), json.MustEncodeString(f.Content, false))
			fileRaw.AddTag(f.AllTags()...)
		} else {
			var err error
			fileRaw, err = file.ToRawFile()
			assert.NoError(t, err)
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
			AddTag(model.FileTypeJson),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJson),
		filesystem.
			NewRawFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2","task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false,"enabled":false}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJson),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/phase.json`,
				`{"name":"Phase With Deps","dependsOn":["001-phase"]}`,
			).
			AddTag(model.FileKindPhaseConfig).
			AddTag(model.FileTypeJson),
		filesystem.
			NewRawFile(
				phasesDir+`/002-phase-with-deps/001-task-3/task.json`,
				`{"name":"Task 3","task":{"mode":"run","configPath":"extractor/target-config-3"},"continueOnFailure":false,"enabled":true}`,
			).
			AddTag(model.FileKindTaskConfig).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(configDir+`/meta.json`, `{"name":"My Orchestration","isDisabled":false}`).
			AddTag(model.FileKindObjectMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(configDir+`/config.json`, `{}`).
			AddTag(model.FileKindObjectConfig).
			AddTag(model.FileTypeJson),
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
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	expectedWarnings := `
WARN  Warning: cannot save orchestrator config "branch/other/orchestrator":
  - cannot save phase "001-phase":
    - cannot save task "001-task-1":
      - config "branch:123/component:foo.bar1/config:123" not found
    - cannot save task "002-task-2":
      - config "branch:123/component:foo.bar2/config:789" not found
  - cannot save phase "002-phase-with-deps":
    - cannot save task "001-task-3":
      - config "branch:123/component:foo.bar2/config:456" not found
`
	assert.Equal(t, strings.TrimLeft(expectedWarnings, "\n"), logger.WarnAndErrorMessages())
}
