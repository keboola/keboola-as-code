package local_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type testMapper struct {
	localChanges []string
}

func (*testMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Content.Set("key", "overwritten")
		config.Content.Set("new", "value")
	}
	return nil
}

func (*testMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Content.Set("parameters", "overwritten")
		config.Content.Set("new", "value")
	}
	return nil
}

func (t *testMapper) OnLocalChange(changes *model.LocalChanges) error {
	for _, objectState := range changes.Created() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`created %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Loaded() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`loaded %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Saved() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`saved %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Deleted() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`deleted %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Persisted() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`persisted %s`, objectState.Desc()))
	}
	for _, action := range changes.Renamed() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`renamed %s`, action.String()))
	}
	return nil
}

func TestLocalSaveMapper(t *testing.T) {
	t.Parallel()
	projectState := newEmptyState(t)
	fs := projectState.Fs()
	uow := projectState.LocalManager().NewUnitOfWork(context.Background())

	// Add test mapper
	testMapperInst := &testMapper{}
	projectState.Mapper().AddMapper(testMapperInst)

	// Test object
	configKey := model.ConfigKey{BranchId: 123, ComponentId: `foo.bar`, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch`, `config`),
			},
		},
		Remote: &model.Config{
			ConfigKey: configKey,
			Name:      "name",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key", Value: "internal value"},
			}),
		},
	}

	// Save object
	uow.SaveObject(configState, configState.Remote, model.ChangedFields{})
	assert.NoError(t, uow.Invoke())

	// File content has been mapped
	configFile, err := fs.ReadFile(filesystem.NewFileDef(filesystem.Join(`branch`, `config`, naming.ConfigFile)).SetDescription(`config file`))
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"key\": \"overwritten\",\n  \"new\": \"value\"\n}", strings.TrimSpace(configFile.Content))

	// OnLocalChange event has been called
	assert.Equal(t, []string{
		`saved config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.localChanges)
}

func TestLocalLoadMapper(t *testing.T) {
	t.Parallel()
	projectState := newEmptyState(t)
	fs := projectState.Fs()
	uow := projectState.LocalManager().NewUnitOfWork(context.Background())

	// Add test mapper
	testMapperInst := &testMapper{}
	projectState.Mapper().AddMapper(testMapperInst)

	// Init dir
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	inputDir := filesystem.Join(testDir, `..`, `fixtures`, `local`, `minimal`)
	assert.NoError(t, aferofs.CopyFs2Fs(nil, inputDir, fs, ``))

	// Replace placeholders in files
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_PROJECT_ID", "12345")
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", "456")
	testhelper.ReplaceEnvsDir(fs, `/`, envs)

	// Load objects
	m, err := projectManifest.Load(fs)
	assert.NoError(t, err)
	uow.LoadAll(m, m.Filter())
	assert.NoError(t, uow.Invoke())

	// Internal state has been mapped
	configState := projectState.MustGet(model.ConfigKey{BranchId: 111, ComponentId: `ex-generic-v2`, Id: `456`}).(*model.ConfigState)
	assert.Equal(t, `{"parameters":"overwritten","new":"value"}`, json.MustEncodeString(configState.Local.Content, false))

	// OnLocalChange event has been called
	assert.Equal(t, []string{
		`loaded branch "111"`,
		`loaded config "branch:111/component:ex-generic-v2/config:456"`,
	}, testMapperInst.localChanges)
}

func newEmptyState(t *testing.T) *state.State {
	t.Helper()
	d := testdeps.New()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(corefiles.NewMapper(mockedState))
	return mockedState
}
