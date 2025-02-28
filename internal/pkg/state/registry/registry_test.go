package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

func TestNewState(t *testing.T) {
	t.Parallel()
	s := New(knownpaths.NewNop(t.Context()), naming.NewRegistry(), NewComponentsMap(nil), SortByPath)
	assert.NotNil(t, s)
}

func TestStateComponents(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.NotNil(t, s.Components())
}

func TestStateAll(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.Len(t, s.All(), 6)
}

func TestStateBranches(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.Len(t, s.Branches(), 2)
}

func TestStateConfigs(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.Len(t, s.Configs(), 2)
}

func TestStateConfigRows(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.Len(t, s.ConfigRows(), 2)
}

func TestStateConfigsFrom(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.Len(t, s.ConfigsFrom(BranchKey{ID: 123}), 2)
	assert.Empty(t, s.ConfigsFrom(BranchKey{ID: 567}))
	assert.Empty(t, s.ConfigsFrom(BranchKey{ID: 111}))
}

func TestStateConfigRowsFrom(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.Len(t, s.ConfigRowsFrom(ConfigKey{BranchID: 123, ComponentID: "keboola.bar", ID: `678`}), 2)
	assert.Empty(t, s.ConfigRowsFrom(ConfigKey{BranchID: 123, ComponentID: "keboola.bar", ID: `345`}))
	assert.Empty(t, s.ConfigRowsFrom(ConfigKey{BranchID: 123, ComponentID: "keboola.bar", ID: `111`}))
}

func TestStateGet(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	state, found := s.Get(BranchKey{ID: 567})
	assert.NotNil(t, state)
	assert.True(t, found)
}

func TestStateGetNotFound(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	state, found := s.Get(BranchKey{ID: 111})
	assert.Nil(t, state)
	assert.False(t, found)
}

func TestStateMustGet(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.Equal(t, "Foo Bar Branch", s.MustGet(BranchKey{ID: 567}).ObjectName())
}

func TestStateMustGetNotFound(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop(t.Context()))
	assert.PanicsWithError(t, `branch "111" not found`, func() {
		s.MustGet(BranchKey{ID: 111})
	})
}

func TestStateTrackRecordNotPersisted(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	ctx := t.Context()
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, `foo`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar2`, `foo`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar3`, `foo`)))
	paths, err := knownpaths.New(ctx, fs)
	require.NoError(t, err)
	s := newTestState(t, paths)

	record := &ConfigManifest{
		RecordState: RecordState{
			Persisted: false,
			Invalid:   true,
		},
	}
	record.AbsPath = NewAbsPath(``, `foo`)
	record.RelatedPaths = []string{`bar1`, `bar2`}

	// Tracked are only paths from persisted records.
	s.TrackObjectPaths(record)
	assert.Empty(t, s.TrackedPaths())
	assert.Equal(t, []string{`foo`, `foo/bar1`, `foo/bar2`, `foo/bar3`}, s.UntrackedPaths())
}

func TestStateTrackRecordValid(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	ctx := t.Context()
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, `foo`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar2`, `foo`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar3`, `foo`)))
	paths, err := knownpaths.New(ctx, fs)
	require.NoError(t, err)
	s := newTestState(t, paths)

	record := &ConfigManifest{
		RecordState: RecordState{
			Persisted: true,
			Invalid:   false,
		},
	}
	record.AbsPath = NewAbsPath(``, `foo`)
	record.RelatedPaths = []string{`bar1`, `bar2`}

	// For valid records, we will mark as tracked only those files that have been loaded.
	s.TrackObjectPaths(record)
	assert.Equal(t, []string{`foo`, `foo/bar1`, `foo/bar2`}, s.TrackedPaths())
	assert.Equal(t, []string{`foo/bar3`}, s.UntrackedPaths())
}

func TestStateTrackRecordInvalid(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fs := aferofs.NewMemoryFs()
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar1`, `foo`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar2`, `foo`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`foo/bar3`, `foo`)))
	paths, err := knownpaths.New(ctx, fs)
	require.NoError(t, err)
	s := newTestState(t, paths)

	record := &ConfigManifest{
		RecordState: RecordState{
			Persisted: true,
			Invalid:   true,
		},
	}
	record.AbsPath = NewAbsPath(``, `foo`)

	// We do not load files for invalid records.
	// Therefore, we mark all files from the object directory as tracked.
	// This will prevent duplicate error -> untracked files found.
	// The user must primarily fix why the record is invalid.
	s.TrackObjectPaths(record)
	assert.Equal(t, []string{`foo`, `foo/bar1`, `foo/bar2`, `foo/bar3`}, s.TrackedPaths())
	assert.Empty(t, s.UntrackedPaths())
}

func TestRegistry_GetPath(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	registry := New(knownpaths.NewNop(ctx), naming.NewRegistry(), NewComponentsMap(nil), SortByPath)

	// Not found
	path, found := registry.GetPath(BranchKey{ID: 123})
	assert.Empty(t, path)
	assert.False(t, found)

	// Add branch
	require.NoError(t, registry.Set(&BranchState{
		BranchManifest: &BranchManifest{
			BranchKey: BranchKey{ID: 123},
			Paths: Paths{
				AbsPath: NewAbsPath(``, `my-branch`),
			},
		},
	}))

	// Found
	path, found = registry.GetPath(BranchKey{ID: 123})
	assert.Equal(t, NewAbsPath(``, `my-branch`), path)
	assert.True(t, found)
}

func TestRegistry_GetByPath(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	registry := New(knownpaths.NewNop(ctx), naming.NewRegistry(), NewComponentsMap(nil), SortByPath)

	// Not found
	objectState, found := registry.GetByPath(`my-branch`)
	assert.Nil(t, objectState)
	assert.False(t, found)

	// Add branch
	branchState := &BranchState{
		BranchManifest: &BranchManifest{
			BranchKey: BranchKey{ID: 123},
			Paths: Paths{
				AbsPath: NewAbsPath(``, `my-branch`),
			},
		},
	}
	require.NoError(t, registry.Set(branchState))

	// Found
	objectState, found = registry.GetByPath(`my-branch`)
	assert.Equal(t, branchState, objectState)
	assert.True(t, found)
}

func newTestState(t *testing.T, paths *knownpaths.Paths) *Registry {
	t.Helper()
	registry := New(paths, naming.NewRegistry(), NewComponentsMap(nil), SortByPath)
	assert.NotNil(t, registry)

	// Branch 1
	branch1Key := BranchKey{ID: 123}
	branch1 := &BranchState{
		BranchManifest: &BranchManifest{
			BranchKey: branch1Key,
		},
		Local: &Branch{
			Name:      "Main",
			IsDefault: true,
		},
	}
	require.NoError(t, registry.Set(branch1))

	// Branch 2
	branch2Key := BranchKey{ID: 567}
	branch2 := &BranchState{
		BranchManifest: &BranchManifest{
			BranchKey: branch2Key,
		},
		Local: &Branch{
			Name:      "Foo Bar Branch",
			IsDefault: false,
		},
	}
	require.NoError(t, registry.Set(branch2))

	// Config 1
	config1Key := ConfigKey{BranchID: 123, ComponentID: "keboola.foo", ID: `345`}
	config1 := &ConfigState{
		ConfigManifest: &ConfigManifest{ConfigKey: config1Key},
		Local: &Config{
			Name: "Config 1",
		},
	}
	require.NoError(t, registry.Set(config1))

	// Config 2
	config2Key := ConfigKey{BranchID: 123, ComponentID: "keboola.bar", ID: `678`}
	config2 := &ConfigState{
		ConfigManifest: &ConfigManifest{ConfigKey: config2Key},
		Local: &Config{
			Name: "Config 2",
		},
	}
	require.NoError(t, registry.Set(config2))

	// Config Row 1
	row1Key := ConfigRowKey{BranchID: 123, ComponentID: "keboola.bar", ConfigID: `678`, ID: `12`}
	row1 := &ConfigRowState{
		ConfigRowManifest: &ConfigRowManifest{ConfigRowKey: row1Key},
		Local: &ConfigRow{
			Name: "Config Row 1",
		},
	}
	require.NoError(t, registry.Set(row1))

	// Config Row 2
	row2Key := ConfigRowKey{BranchID: 123, ComponentID: "keboola.bar", ConfigID: `678`, ID: `34`}
	row2 := &ConfigRowState{
		ConfigRowManifest: &ConfigRowManifest{ConfigRowKey: row2Key},
		Local: &ConfigRow{
			Name: "Config Row 2",
		},
	}
	require.NoError(t, registry.Set(row2))

	return registry
}
