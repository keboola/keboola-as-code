package diff

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/model"
	"testing"
)

func TestDiffOnlyInLocal(t *testing.T) {
	projectDir := t.TempDir()
	state := model.NewState(projectDir)
	branch := &model.Branch{}
	manifest := &model.BranchManifest{}
	state.SetBranchLocalState(branch, manifest)
	d := NewDiffer(state)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInLocal, result.State)
	assert.Equal(t, []string{}, result.ChangedFields)
	assert.Same(t, branch, result.ObjectState.LocalState().(*model.Branch))
	assert.Same(t, manifest, result.ObjectState.ManifestState().(*model.BranchManifest))
}

func TestDiffOnlyInRemote(t *testing.T) {
	projectDir := t.TempDir()
	state := model.NewState(projectDir)
	branch := &model.Branch{}
	state.SetBranchRemoteState(branch)
	d := NewDiffer(state)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInRemote, result.State)
	assert.Equal(t, []string{}, result.ChangedFields)
	assert.Same(t, branch, result.ObjectState.RemoteState().(*model.Branch))
}

func TestDiffEqual(t *testing.T) {
	projectDir := t.TempDir()
	state := model.NewState(projectDir)
	branchRemote := &model.Branch{
		Id:          123,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		Id:          123,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	manifest := &model.BranchManifest{}
	state.SetBranchRemoteState(branchRemote)
	state.SetBranchLocalState(branchLocal, manifest)
	d := NewDiffer(state)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultEqual, result.State)
	assert.Equal(t, []string{}, result.ChangedFields)
	assert.Same(t, branchRemote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result.ObjectState.LocalState().(*model.Branch))
	assert.Equal(t, manifest, result.ObjectState.ManifestState().(*model.BranchManifest))
}

func TestDiffNotEqual(t *testing.T) {
	projectDir := t.TempDir()
	state := model.NewState(projectDir)
	branchRemote := &model.Branch{
		Id:          123,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		Id:          123,
		Name:        "changed",
		Description: "description",
		IsDefault:   true,
	}
	manifest := &model.BranchManifest{}
	state.SetBranchRemoteState(branchRemote)
	state.SetBranchLocalState(branchLocal, manifest)
	d := NewDiffer(state)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)
	assert.Equal(t, []string{"name", "isDefault"}, result.ChangedFields)
	assert.Equal(t, map[string]string{
		"name":      "  string(\n- \t\"name\",\n+ \t\"changed\",\n  )\n",
		"isDefault": "  bool(\n- \tfalse,\n+ \ttrue,\n  )\n",
	}, result.Differences)
	assert.Same(t, branchRemote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result.ObjectState.LocalState().(*model.Branch))
	assert.Equal(t, manifest, result.ObjectState.ManifestState().(*model.BranchManifest))
}

func TestDiffEqualConfig(t *testing.T) {
	projectDir := t.TempDir()
	state := model.NewState(projectDir)
	branchRemote := &model.Branch{
		Id:          123,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		Id:          123,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchManifest := &model.BranchManifest{}
	configRemote := &model.Config{
		BranchId:          123,
		ComponentId:       "foo",
		Id:                "456",
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	}
	configLocal := &model.Config{
		BranchId:          123,
		ComponentId:       "foo",
		Id:                "456",
		Name:              "name",
		Description:       "description",
		ChangeDescription: "local", // no diff:"true" tag
	}
	configManifest := &model.ConfigManifest{}
	state.SetBranchRemoteState(branchRemote)
	state.SetBranchLocalState(branchLocal, branchManifest)
	state.SetConfigRemoteState(configRemote)
	state.SetConfigLocalState(configLocal, configManifest)
	d := NewDiffer(state)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)
	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.Equal(t, []string{}, result1.ChangedFields)
	assert.Same(t, branchRemote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result1.ObjectState.LocalState().(*model.Branch))
	assert.Equal(t, branchManifest, result1.ObjectState.ManifestState().(*model.BranchManifest))
	result2 := results.Results[1]
	assert.Equal(t, ResultEqual, result2.State)
	assert.Equal(t, []string{}, result2.ChangedFields)
	assert.Same(t, configRemote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configLocal, result2.ObjectState.LocalState().(*model.Config))
	assert.Equal(t, configManifest, result2.ObjectState.ManifestState().(*model.ConfigManifest))
}

func TestDiffNotEqualConfig(t *testing.T) {
	projectDir := t.TempDir()
	state := model.NewState(projectDir)
	branchRemote := &model.Branch{
		Id:          123,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchLocal := &model.Branch{
		Id:          123,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	branchManifest := &model.BranchManifest{}
	configRemote := &model.Config{
		BranchId:          123,
		ComponentId:       "foo",
		Id:                "456",
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	}
	configLocal := &model.Config{
		BranchId:          123,
		ComponentId:       "foo",
		Id:                "456",
		Name:              "changed",
		Description:       "changed",
		ChangeDescription: "local", // no diff:"true" tag
	}
	configManifest := &model.ConfigManifest{}
	state.SetBranchRemoteState(branchRemote)
	state.SetBranchLocalState(branchLocal, branchManifest)
	state.SetConfigRemoteState(configRemote)
	state.SetConfigLocalState(configLocal, configManifest)
	d := NewDiffer(state)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)
	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.Equal(t, []string{}, result1.ChangedFields)
	assert.Same(t, branchRemote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchLocal, result1.ObjectState.LocalState().(*model.Branch))
	assert.Equal(t, branchManifest, result1.ObjectState.ManifestState().(*model.BranchManifest))
	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, []string{"name", "description"}, result2.ChangedFields)
	assert.Same(t, configRemote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configLocal, result2.ObjectState.LocalState().(*model.Config))
	assert.Equal(t, configManifest, result2.ObjectState.ManifestState().(*model.ConfigManifest))
}
