package orchestrator_test

import (
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

	// Check generated files - now using developer-friendly format with single pipeline.yml
	configDir := orchestratorConfigState.Path()

	// Expected YAML content for the pipeline
	// Note: Config metadata (name, description, _keboola) is in _config.yml, not here
	expectedPipelineYAML := `version: 2
phases:
    - name: Phase
      tasks:
        - name: Task 1
          component: foo.bar1
          config: extractor/target-config-1
          path: branch/extractor/target-config-1
        - name: Task 2 - disabled
          component: foo.bar2
          config: extractor/target-config-2
          path: branch/extractor/target-config-2
          enabled: false
        - name: Task 3 - disabled without configId
          component: foo.bar2
          config: ""
          enabled: false
    - name: Phase With Deps
      depends_on:
        - Phase
      tasks:
        - name: Task 4
          component: foo.bar2
          config: extractor/target-config-3
          path: branch/extractor/target-config-3
        - name: Task 5 - configData
          component: foo.bar3
          config: ""
          parameters:
            params: value
`

	// Expected _config.yml content
	expectedConfigYAML := `version: 2
name: My Orchestration
_keboola:
    component_id: keboola.orchestrator
    config_id: "456"
`

	assert.Equal(t, []filesystem.File{
		filesystem.NewRawFile(configDir+`/pipeline.yml`, expectedPipelineYAML).
			AddTag(model.FileTypeYaml),
		filesystem.NewRawFile(configDir+`/_config.yml`, expectedConfigYAML).
			AddTag(model.FileKindObjectConfig).
			AddTag(model.FileTypeYaml),
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
	// With the new pipeline.yml format, config paths are stored as-is without validation
	// so there are no warnings when configs are not found
	assert.Empty(t, logger.WarnAndErrorMessages())
}
