package ignore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/registry"
)

func Test_loadFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	projectState := newTestRegistry(t)

	fs := aferofs.NewMemoryFs()
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, "keboola.bar/678/34\nkeboola.foo/345")))

	file, err := LoadFile(ctx, fs, projectState, "foo/bar1")
	require.NoError(t, err)

	assert.Equal(t, "keboola.bar/678/34\nkeboola.foo/345", file.rawStringPattern)
}

func newTestRegistry(t *testing.T) *registry.Registry {
	t.Helper()

	r := registry.New(knownpaths.NewNop(context.Background()), naming.NewRegistry(), model.NewComponentsMap(nil), model.SortByPath)
	// Branch 1
	branch1Key := model.BranchKey{ID: 123}
	branch1 := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branch1Key,
		},
		Local: &model.Branch{
			Name:      "Main",
			IsDefault: true,
		},
	}
	assert.NoError(t, r.Set(branch1))

	// Branch 2
	branch2Key := model.BranchKey{ID: 567}
	branch2 := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branch2Key,
		},
		Local: &model.Branch{
			Name:      "Foo Bar Branch",
			IsDefault: false,
		},
	}
	assert.NoError(t, r.Set(branch2))

	// Config 1
	config1Key := model.ConfigKey{BranchID: 123, ComponentID: "keboola.foo", ID: `345`}
	config1 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: config1Key},
		Local: &model.Config{
			Name: "Config 1",
		},
	}
	assert.NoError(t, r.Set(config1))

	// Config 2
	config2Key := model.ConfigKey{BranchID: 123, ComponentID: "keboola.bar", ID: `678`}
	config2 := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: config2Key},
		Local: &model.Config{
			Name: "Config 2",
		},
	}
	assert.NoError(t, r.Set(config2))

	// Config Row 1
	row1Key := model.ConfigRowKey{BranchID: 123, ComponentID: "keboola.bar", ConfigID: `678`, ID: `12`}
	row1 := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: row1Key},
		Local: &model.ConfigRow{
			Name: "Config Row 1",
		},
	}
	assert.NoError(t, r.Set(row1))

	// Config Row 2
	row2Key := model.ConfigRowKey{BranchID: 123, ComponentID: "keboola.bar", ConfigID: `678`, ID: `34`}
	row2 := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: row2Key},
		Local: &model.ConfigRow{
			Name: "Config Row 2",
		},
	}
	assert.NoError(t, r.Set(row2))

	return r
}
