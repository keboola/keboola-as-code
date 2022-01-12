package orchestrator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
)

func TestMapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Recipe
	orchestratorConfigState := createLocalSaveFixtures(t, state, true)
	recipe := fixtures.NewLocalSaveRecipe(orchestratorConfigState.Manifest(), orchestratorConfigState.Remote)

	// Save
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Minify JSON + remove file description
	var files []*filesystem.File
	for _, fileRaw := range recipe.Files.All() {
		var file *filesystem.File
		if f, ok := fileRaw.File().(*filesystem.JsonFile); ok {
			file = filesystem.NewFile(f.GetPath(), json.MustEncodeString(f.Content, false))
		} else {
			var err error
			file, err = fileRaw.ToFile()
			assert.NoError(t, err)
			file.SetDescription(``)
		}
		files = append(files, file)
	}

	// Check generated files
	phasesDir := state.NamingGenerator().PhasesDir(orchestratorConfigState.Path())
	assert.Equal(t, []*filesystem.File{
		filesystem.NewFile(`meta.json`, `{}`),
		filesystem.NewFile(`config.json`, `{}`),
		filesystem.NewFile(`description.md`, ``),
		filesystem.NewFile(phasesDir+`/.gitkeep`, ``),
		filesystem.
			NewFile(
				phasesDir+`/001-phase/phase.json`,
				`{"name":"Phase","dependsOn":[],"foo":"bar"}`,
			),
		filesystem.
			NewFile(
				phasesDir+`/001-phase/001-task-1/task.json`,
				`{"name":"Task 1","task":{"mode":"run","configPath":"extractor/target-config-1"},"continueOnFailure":false,"enabled":true}`,
			),
		filesystem.
			NewFile(
				phasesDir+`/001-phase/002-task-2/task.json`,
				`{"name":"Task 2","task":{"mode":"run","configPath":"extractor/target-config-2"},"continueOnFailure":false,"enabled":false}`,
			),
		filesystem.
			NewFile(
				phasesDir+`/002-phase-with-deps/phase.json`,
				`{"name":"Phase With Deps","dependsOn":["001-phase"]}`,
			),
		filesystem.
			NewFile(
				phasesDir+`/002-phase-with-deps/001-task-3/task.json`,
				`{"name":"Task 3","task":{"mode":"run","configPath":"extractor/target-config-3"},"continueOnFailure":false,"enabled":true}`,
			),
	}, files)
}

func TestMapBeforeLocalSaveWarnings(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Recipe
	orchestratorConfigState := createLocalSaveFixtures(t, state, false)
	recipe := fixtures.NewLocalSaveRecipe(orchestratorConfigState.Manifest(), orchestratorConfigState.Remote)

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
