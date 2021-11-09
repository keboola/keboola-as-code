package diff

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestDiffOnlyInLocal(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)
	branchKey := model.BranchKey{Id: 123}
	branch := &model.Branch{BranchKey: branchKey}
	branchState, err := projectState.CreateFrom(&model.BranchManifest{BranchKey: branchKey})
	assert.NoError(t, err)
	branchState.SetLocalState(branch)

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInLocal, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())
	assert.Same(t, branch, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffOnlyInRemote(t *testing.T) {
	t.Parallel()
	branchKey := model.BranchKey{Id: 123}
	branch := &model.Branch{BranchKey: branchKey}
	projectState := createProjectState(t)
	branchState, err := projectState.CreateFrom(&model.BranchManifest{BranchKey: branchKey})
	assert.NoError(t, err)
	branchState.SetRemoteState(branch)

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInRemote, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())
	assert.Same(t, branch, result.ObjectState.RemoteState().(*model.Branch))
}

func TestDiffEqual(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)
	branchKey := model.BranchKey{
		Id: 123,
	}
	branchRemote := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchState, err := projectState.CreateFrom(&model.BranchManifest{BranchKey: branchKey})
	assert.NoError(t, err)
	branchState.SetRemoteState(branchRemote)
	branchState.SetLocalState(branchLocal)

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultEqual, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())
	assert.Same(t, branchRemote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffNotEqual(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)
	branchKey := model.BranchKey{
		Id: 123,
	}
	branchRemote := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey:   branchKey,
		Name:        "changed",
		Description: "description",
		IsDefault:   true,
	}
	branchState, err := projectState.CreateFrom(&model.BranchManifest{BranchKey: branchKey})
	assert.NoError(t, err)
	branchState.SetRemoteState(branchRemote)
	branchState.SetLocalState(branchLocal)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)
	assert.Equal(t, `isDefault, name`, result.ChangedFields.String())
	assert.Equal(t, "  - name\n  + changed", result.ChangedFields.Get("name").Diff())
	assert.Equal(t, "  - false\n  + true", result.ChangedFields.Get("isDefault").Diff())
	assert.Same(t, branchRemote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffEqualConfig(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	projectState.Components().Set(component)

	branchKey := model.BranchKey{
		Id: 123,
	}
	branchRemote := &model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	}

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo-bar",
		Id:          "456",
	}
	configRemote := &model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	}
	configLocal := &model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "local", // no diff:"true" tag
	}
	branchState, err := projectState.CreateFrom(&model.BranchManifest{BranchKey: branchKey})
	assert.NoError(t, err)
	branchState.SetRemoteState(branchRemote)
	branchState.SetLocalState(branchLocal)
	configState, err := projectState.CreateFrom(&model.ConfigManifest{ConfigKey: configKey})
	assert.NoError(t, err)
	configState.SetRemoteState(configRemote)
	configState.SetLocalState(configLocal)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)
	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())
	assert.Same(t, branchRemote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result1.ObjectState.LocalState().(*model.Branch))
	result2 := results.Results[1]
	assert.Equal(t, ResultEqual, result2.State)
	assert.True(t, result2.ChangedFields.IsEmpty())
	assert.Same(t, configRemote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configLocal, result2.ObjectState.LocalState().(*model.Config))
}

func TestDiffNotEqualConfig(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	projectState.Components().Set(component)

	branchKey := model.BranchKey{
		Id: 123,
	}
	branchRemote := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo-bar",
		Id:          "456",
	}
	configRemote := &model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	}
	configLocal := &model.Config{
		ConfigKey:         configKey,
		Name:              "changed",
		Description:       "changed",
		ChangeDescription: "local", // no diff:"true" tag
	}
	branchState, err := projectState.CreateFrom(&model.BranchManifest{BranchKey: branchKey})
	assert.NoError(t, err)
	branchState.SetRemoteState(branchRemote)
	branchState.SetLocalState(branchLocal)

	configState, err := projectState.CreateFrom(&model.ConfigManifest{ConfigKey: configKey})
	assert.NoError(t, err)
	configState.SetRemoteState(configRemote)
	configState.SetLocalState(configLocal)

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())
	assert.Same(t, branchRemote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result1.ObjectState.LocalState().(*model.Branch))

	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, `description, name`, result2.ChangedFields.String())
	assert.Same(t, configRemote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configLocal, result2.ObjectState.LocalState().(*model.Config))
}

func TestDiffNotEqualConfigConfiguration(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	projectState.Components().Set(component)

	branchKey := model.BranchKey{
		Id: 123,
	}
	branchRemote := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo-bar",
		Id:          "456",
	}
	configRemote := &model.Config{
		ConfigKey: configKey,
		Content: utils.PairsToOrderedMap([]utils.Pair{
			{
				Key: "foo",
				Value: utils.PairsToOrderedMap([]utils.Pair{
					{Key: "bar", Value: "123"},
				}),
			},
		}),
	}
	configLocal := &model.Config{
		ConfigKey: configKey,
		Content: utils.PairsToOrderedMap([]utils.Pair{
			{
				Key: "foo",
				Value: utils.PairsToOrderedMap([]utils.Pair{
					{Key: "bar", Value: "456"},
				}),
			},
		}),
	}
	branchState, err := projectState.CreateFrom(&model.BranchManifest{BranchKey: branchKey})
	assert.NoError(t, err)
	branchState.SetRemoteState(branchRemote)
	branchState.SetLocalState(branchLocal)

	configState, err := projectState.CreateFrom(&model.ConfigManifest{ConfigKey: configKey})
	assert.NoError(t, err)
	configState.SetRemoteState(configRemote)
	configState.SetLocalState(configLocal)

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())
	assert.Same(t, branchRemote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result1.ObjectState.LocalState().(*model.Branch))

	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, `configuration`, result2.ChangedFields.String())
	assert.Equal(t, `foo.bar`, result2.ChangedFields.Get(`configuration`).Paths())
	assert.Same(t, configRemote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configLocal, result2.ObjectState.LocalState().(*model.Config))
}

func TestDiffRelations(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	// Target object
	targetKey := fixtures.MockedKey{
		Id: `123`,
	}
	_, err := projectState.CreateFrom(&fixtures.MockedRecord{
		MockedKey: targetKey,
		PathValue: `path/to/target`,
	})
	assert.NoError(t, err)

	objectKey := fixtures.MockedKey{
		Id: `345`,
	}

	// Remote state
	rObject := &fixtures.MockedObject{
		MockedKey: objectKey,
		Relations: model.Relations{
			&fixtures.MockedApiSideRelation{
				OtherSide: fixtures.MockedKey{Id: `123`},
			},
			&fixtures.MockedApiSideRelation{
				OtherSide: fixtures.MockedKey{Id: `001`},
			},
			&fixtures.MockedManifestSideRelation{
				OtherSide: fixtures.MockedKey{Id: `foo`},
			},
		},
	}
	assert.NoError(t, err)

	// Local state
	lObject := &fixtures.MockedObject{
		MockedKey: objectKey,
		Relations: model.Relations{
			&fixtures.MockedApiSideRelation{
				OtherSide: fixtures.MockedKey{Id: `001`},
			},
			&fixtures.MockedApiSideRelation{
				OtherSide: fixtures.MockedKey{Id: `002`},
			},
			&fixtures.MockedManifestSideRelation{
				OtherSide: fixtures.MockedKey{Id: `bar`},
			},
		},
	}

	// Object state
	objectState, err := projectState.CreateFrom(&fixtures.MockedRecord{
		MockedKey: objectKey,
		PathValue: `path/to/object`,
	})
	assert.NoError(t, err)
	objectState.SetLocalState(lObject)
	objectState.SetRemoteState(rObject)

	differ := NewDiffer(projectState)
	reporter := differ.diffValues(objectState, rObject.Relations, lObject.Relations)
	expected := `
  - manifest side relation mocked key "foo"
  + manifest side relation mocked key "bar"
  - api side relation "path/to/target"
  + api side relation mocked key "002"
`
	assert.Equal(t, strings.Trim(expected, "\n"), reporter.String())
	assert.Equal(t, []string{"InManifest", "InApi"}, reporter.Paths()) // see model.RelationsBySide
}

func createProjectState(t *testing.T) *state.State {
	t.Helper()

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	m, err := manifest.NewManifest(1, `foo.bar`, fs)
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	storageApi, _, _ := testapi.TestMockedStorageApi()
	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	options := state.NewOptions(m, storageApi, schedulerApi, context.Background(), logger)
	options.LoadLocalState = false
	options.LoadRemoteState = false
	s, _ := state.LoadState(options)
	return s
}
