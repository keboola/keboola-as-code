package state

import (
	"github.com/jarcoal/httpmock"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPersistNoChange(t *testing.T) {
	projectDir := initProjectDir(t)
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
	api, _ := remote.TestMockedStorageApi(t)

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic Extractor",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)

	state := NewState(projectDir, api, m)
	assert.NotNil(t, state)
	state.LoadLocalState()
	assert.Empty(t, state.LocalErrors().Errors())
	assert.Empty(t, state.UntrackedPaths())

	persisted, err := state.Persist()
	assert.NoError(t, err)
	assert.Empty(t, persisted)
}

func TestPersistCreatedConfig(t *testing.T) {
	projectDir := initProjectDir(t)
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
	api, _ := remote.TestMockedStorageApi(t)

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic Extractor",
	})
	assert.NoError(t, err)
	generateNewIdResponser, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id": "12345",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)
	httpmock.RegisterResponder("POST", `=~/storage/tickets`, generateNewIdResponser)

	configDir := filepath.Join(projectDir, `main`, `extractor`, `ex-generic-v2`, `new-config`)
	assert.NoError(t, os.Mkdir(configDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `config.json`), []byte(`{"key": "value"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `meta.json`), []byte(`{"name": "foo", "description": "bar"}`), 0644))

	state := NewState(projectDir, api, m)
	assert.NotNil(t, state)
	state.LoadLocalState()
	assert.Empty(t, state.LocalErrors().Errors())
	assert.Equal(t, []string{
		"main/extractor/ex-generic-v2/new-config",
		"main/extractor/ex-generic-v2/new-config/config.json",
		"main/extractor/ex-generic-v2/new-config/meta.json",
	}, state.UntrackedPaths())
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 1)
	assert.Len(t, state.All(), 2)

	persisted, err := state.Persist()
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"main/extractor/ex-generic-v2/new-config",
	}, persisted)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 2)
	assert.Len(t, state.All(), 3)
	configKey := model.ConfigKey{BranchId: 111, ComponentId: "ex-generic-v2", Id: "12345"}
	assert.Equal(
		t,
		&ConfigState{
			ConfigManifest: &manifest.ConfigManifest{
				ConfigKey: configKey,
				RecordState: manifest.RecordState{
					Invalid:   false,
					Persisted: true,
				},
				Paths: manifest.Paths{
					ParentPath: "main",
					Path:       "extractor/ex-generic-v2/new-config",
				},
			},
			Component: &model.Component{
				ComponentKey: model.ComponentKey{
					Id: configKey.ComponentId,
				},
				Type: "extractor",
				Name: "Generic Extractor",
			},
			Remote: nil,
			Local: &model.Config{
				ConfigKey:   configKey,
				Name:        "foo",
				Description: "bar",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key:   "key",
						Value: "value",
					},
				}),
			},
		},
		state.GetConfig(configKey, false),
	)
}

func initProjectDir(t *testing.T) string {
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", "456")

	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := t.TempDir()
	err := copy.Copy(filepath.Join(testDir, `..`, `fixtures`, `local`, `minimal`), projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	utils.ReplaceEnvsDir(projectDir)

	return projectDir
}
