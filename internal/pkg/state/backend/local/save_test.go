package local_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestUnitOfWork_Save_Mapper(t *testing.T) {
	t.Parallel()
	testMapperInst := &testMapper{}
	uow, fs, manifest, s := newTestUow(t, testMapperInst)

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

	// AfterLocalOperation event has been called
	assert.Equal(t, []string{
		`created config "branch:123/component:foo.bar/config:456"`,
		`saved config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.localChanges)
}

func TestUnitOfWork_Save_Relations(t *testing.T) {
	assert.Fail(t, "TODO")

	//t.Parallel()
	//d := dependencies.NewTestContainer()
	//logger := d.DebugLogger()
	//mockedState := d.EmptyState()
	//mockedState.Mapper().AddMapper(configmetadata.NewMapper(mockedState, d))
	//
	//configKey := model.ConfigKey{
	//	BranchId:    123,
	//	ComponentId: model.ComponentId("keboola.snowflake-transformation"),
	//	Id:          `456`,
	//}
	//configState := &model.ConfigState{
	//	ConfigManifest: &model.ConfigManifest{
	//		ConfigKey: configKey,
	//	},
	//	Local: &model.Config{
	//		ConfigKey: configKey,
	//		Name:      "My Config",
	//		Content:   orderedmap.New(),
	//		Metadata:  map[string]string{"KBC.KaC.Meta1": "val1", "KBC.KaC.Meta2": "val2"},
	//	},
	//}
	//
	//recipe := model.NewLocalSaveRecipe(configState.Manifest(), configState.Local, model.NewChangedFields())
	//assert.NoError(t, mockedState.Mapper().MapBeforeLocalSave(recipe))
	//assert.Empty(t, logger.WarnAndErrorMessages())
	//
	//configManifest := recipe.ObjectManifest.(*model.ConfigManifest)
	//assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
	//	{Key: "KBC.KaC.Meta1", Value: "val1"},
	//	{Key: "KBC.KaC.Meta2", Value: "val2"},
	//}), configManifest.Metadata)
}
