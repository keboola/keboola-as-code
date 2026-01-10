package local_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type testMapper struct {
	localChanges []string
}

func (*testMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		// Modify name which will be reflected in the _config.yml
		config.Name = "overwritten-name"
		// Modify parameters which will be extracted to _config.yml
		params := orderedmap.New()
		params.Set("key", "overwritten")
		params.Set("new", "value")
		config.Content.Set("parameters", params)
	}
	return nil
}

func (*testMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Content.Set("parameters", "overwritten")
		config.Content.Set("new", "value")
	}
	return nil
}

func (t *testMapper) AfterLocalOperation(ctx context.Context, changes *model.LocalChanges) error {
	for _, objectState := range changes.Loaded() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`loaded %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Persisted() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`persisted %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Created() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`created %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Updated() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`updated %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Saved() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`saved %s`, objectState.Desc()))
	}
	for _, action := range changes.Renamed() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`renamed %s`, action.String()))
	}
	for _, objectState := range changes.Deleted() {
		t.localChanges = append(t.localChanges, fmt.Sprintf(`deleted %s`, objectState.Desc()))
	}
	return nil
}

func TestLocalSaveMapper(t *testing.T) {
	t.Parallel()
	projectState := newEmptyState(t)
	fs := projectState.ObjectsRoot()
	uow := projectState.LocalManager().NewUnitOfWork(t.Context())

	// Add test mapper
	testMapperInst := &testMapper{}
	projectState.Mapper().AddMapper(testMapperInst)

	// Test object
	configKey := model.ConfigKey{BranchID: 123, ComponentID: `foo.bar`, ID: `456`}
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
	require.NoError(t, uow.Invoke())

	// File content has been mapped - now using unified _config.yml format
	configFile, err := fs.ReadFile(t.Context(), filesystem.NewFileDef(filesystem.Join(`branch`, `config`, naming.ConfigYAMLFile)).SetDescription(`config file`))
	require.NoError(t, err)
	// Check that the YAML content includes the mapped values
	assert.Contains(t, configFile.Content, "name: overwritten-name")
	assert.Contains(t, configFile.Content, "key: overwritten")
	assert.Contains(t, configFile.Content, "new: value")

	// AfterLocalOperation event has been called
	assert.Equal(t, []string{
		`created config "branch:123/component:foo.bar/config:456"`,
		`saved config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.localChanges)
}

func TestLocalLoadMapper(t *testing.T) {
	t.Parallel()
	projectState := newEmptyState(t)
	fs := projectState.ObjectsRoot()
	uow := projectState.LocalManager().NewUnitOfWork(t.Context())

	// Add test mapper
	testMapperInst := &testMapper{}
	projectState.Mapper().AddMapper(testMapperInst)

	// Init dir
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	inputDir := filesystem.Join(testDir, `..`, `..`, `fixtures`, `local`, `minimal`)
	require.NoError(t, aferofs.CopyFs2Fs(nil, inputDir, fs, ``))

	// Replace placeholders in files
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_PROJECT_ID", "12345")
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", "456")
	err := testhelper.ReplaceEnvsDir(t.Context(), fs, `/`, envs)
	require.NoError(t, err)

	// Load objects
	m, err := projectManifest.Load(t.Context(), log.NewNopLogger(), fs, env.Empty(), false)
	require.NoError(t, err)
	uow.LoadAll(m, *m.Filter())
	require.NoError(t, uow.Invoke())

	// Internal state has been mapped
	configState := projectState.MustGet(model.ConfigKey{BranchID: 111, ComponentID: `ex-generic-v2`, ID: `456`}).(*model.ConfigState)
	assert.JSONEq(t, `{"parameters":"overwritten","new":"value"}`, json.MustEncodeString(configState.Local.Content, false))

	// AfterLocalOperation event has been called
	assert.Equal(t, []string{
		`loaded branch "111"`,
		`loaded config "branch:111/component:ex-generic-v2/config:456"`,
	}, testMapperInst.localChanges)
}

func newEmptyState(t *testing.T) *state.State {
	t.Helper()
	d := dependencies.NewMocked(t, t.Context())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(corefiles.NewMapper(mockedState))
	return mockedState
}
