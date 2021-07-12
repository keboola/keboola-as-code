package diff

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"strings"
	"testing"
)

func TestDiffOnlyInLocal(t *testing.T) {
	projectState := createProjectState(t)
	branch := &model.Branch{}
	m := &manifest.BranchManifest{}
	projectState.SetBranchLocalState(branch, m)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInLocal, result.State)
	assert.Equal(t, []string{}, result.ChangedFields)
	assert.Same(t, branch, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffOnlyInRemote(t *testing.T) {
	branch := &model.Branch{}
	projectState := createProjectState(t)
	projectState.SetBranchRemoteState(branch)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInRemote, result.State)
	assert.Equal(t, []string{}, result.ChangedFields)
	assert.Same(t, branch, result.ObjectState.RemoteState().(*model.Branch))
}

func TestDiffEqual(t *testing.T) {
	projectState := createProjectState(t)
	branchRemote := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	m := &manifest.BranchManifest{}
	projectState.SetBranchRemoteState(branchRemote)
	projectState.SetBranchLocalState(branchLocal, m)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultEqual, result.State)
	assert.Equal(t, []string{}, result.ChangedFields)
	assert.Same(t, branchRemote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffNotEqual(t *testing.T) {
	projectState := createProjectState(t)
	branchRemote := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "changed",
		Description: "description",
		IsDefault:   true,
	}
	m := &manifest.BranchManifest{}
	projectState.SetBranchRemoteState(branchRemote)
	projectState.SetBranchLocalState(branchLocal, m)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)
	assert.Equal(t, []string{"name", "isDefault"}, result.ChangedFields)
	assert.Equal(t, "\t- name\n\t+ changed", strings.ReplaceAll(result.Differences["name"], " ", " "))
	assert.Equal(t, "\t- false\n\t+ true", strings.ReplaceAll(result.Differences["isDefault"], " ", " "))
	assert.Same(t, branchRemote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffEqualConfig(t *testing.T) {
	projectState := createProjectState(t)
	branchRemote := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchManifest := &manifest.BranchManifest{}
	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	configRemote := &model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: "foo",
			Id:          "456",
		},
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	}
	configLocal := &model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: "foo",
			Id:          "456",
		},
		Name:              "name",
		Description:       "description",
		ChangeDescription: "local", // no diff:"true" tag
	}
	configManifest := &manifest.ConfigManifest{}
	projectState.SetBranchRemoteState(branchRemote)
	projectState.SetBranchLocalState(branchLocal, branchManifest)
	projectState.SetConfigRemoteState(component, configRemote)
	projectState.SetConfigLocalState(component, configLocal, configManifest)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)
	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.Equal(t, []string{}, result1.ChangedFields)
	assert.Same(t, branchRemote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result1.ObjectState.LocalState().(*model.Branch))
	result2 := results.Results[1]
	assert.Equal(t, ResultEqual, result2.State)
	assert.Equal(t, []string{}, result2.ChangedFields)
	assert.Same(t, configRemote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configLocal, result2.ObjectState.LocalState().(*model.Config))
}

func TestDiffNotEqualConfig(t *testing.T) {
	projectState := createProjectState(t)
	branchRemote := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		BranchKey: model.BranchKey{
			Id: 123,
		},
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchManifest := &manifest.BranchManifest{}
	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	configRemote := &model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: "foo",
			Id:          "456",
		},
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	}
	configLocal := &model.Config{
		ConfigKey: model.ConfigKey{
			BranchId:    123,
			ComponentId: "foo",
			Id:          "456",
		},
		Name:              "changed",
		Description:       "changed",
		ChangeDescription: "local", // no diff:"true" tag
	}
	configManifest := &manifest.ConfigManifest{}
	projectState.SetBranchRemoteState(branchRemote)
	projectState.SetBranchLocalState(branchLocal, branchManifest)
	projectState.SetConfigRemoteState(component, configRemote)
	projectState.SetConfigLocalState(component, configLocal, configManifest)
	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)
	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.Equal(t, []string{}, result1.ChangedFields)
	assert.Same(t, branchRemote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result1.ObjectState.LocalState().(*model.Branch))
	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, []string{"name", "description"}, result2.ChangedFields)
	assert.Same(t, configRemote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configLocal, result2.ObjectState.LocalState().(*model.Config))
}

func createProjectState(t *testing.T) *state.State {
	m, err := manifest.NewManifest(1, "connection.keboola.com", "foo", "bar")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	projectDir := t.TempDir()
	return state.NewState(projectDir, nil, m)
}
