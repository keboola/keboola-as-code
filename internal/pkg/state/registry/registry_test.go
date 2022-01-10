package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
)

func TestNewState(t *testing.T) {
	t.Parallel()
	s := New(knownpaths.NewNop(), naming.NewRegistry(), NewComponentsMap(nil), SortByPath)
	assert.NotNil(t, s)
}

func TestStateComponents(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.NotNil(t, s.Components())
}

func TestStateAll(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.Len(t, s.All(), 6)
}

func TestStateBranches(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.Len(t, s.Branches(), 2)
}

func TestStateConfigs(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.Len(t, s.Configs(), 2)
}

func TestStateConfigRows(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.Len(t, s.ConfigRows(), 2)
}

func TestStateConfigsFrom(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.Len(t, s.ConfigsFrom(BranchKey{Id: 123}), 2)
	assert.Len(t, s.ConfigsFrom(BranchKey{Id: 567}), 0)
	assert.Len(t, s.ConfigsFrom(BranchKey{Id: 111}), 0)
}

func TestStateConfigRowsFrom(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.Len(t, s.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `678`}), 2)
	assert.Len(t, s.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `345`}), 0)
	assert.Len(t, s.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `111`}), 0)
}

func TestStateGet(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	state, found := s.Get(BranchKey{Id: 567})
	assert.NotNil(t, state)
	assert.True(t, found)
}

func TestStateGetNotFound(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	state, found := s.Get(BranchKey{Id: 111})
	assert.Nil(t, state)
	assert.False(t, found)
}

func TestStateMustGet(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.Equal(t, "Foo Bar Branch", s.MustGet(BranchKey{Id: 567}).ObjectName())
}

func TestStateMustGetNotFound(t *testing.T) {
	t.Parallel()
	s := newTestState(t, knownpaths.NewNop())
	assert.PanicsWithError(t, `branch "111" not found`, func() {
		s.MustGet(BranchKey{Id: 111})
	})
}

func TestStateTrackRecordNotPersisted(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar1`, `foo`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar2`, `foo`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar3`, `foo`)))
	paths, err := knownpaths.New(fs)
	assert.NoError(t, err)
	s := newTestState(t, paths)

	record := &ConfigManifest{
		RecordState: RecordState{
			Persisted: false,
			Invalid:   true,
		},
	}
	record.PathInProject = NewPathInProject(``, `foo`)
	record.RelatedPaths = []string{`bar1`, `bar2`}

	// Tracked are only paths from persisted records.
	s.TrackObjectPaths(record)
	assert.Empty(t, s.TrackedPaths())
	assert.Equal(t, []string{`foo`, `foo/bar1`, `foo/bar2`, `foo/bar3`}, s.UntrackedPaths())
}

func TestStateTrackRecordValid(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar1`, `foo`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar2`, `foo`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar3`, `foo`)))
	paths, err := knownpaths.New(fs)
	assert.NoError(t, err)
	s := newTestState(t, paths)

	record := &ConfigManifest{
		RecordState: RecordState{
			Persisted: true,
			Invalid:   false,
		},
	}
	record.PathInProject = NewPathInProject(``, `foo`)
	record.RelatedPaths = []string{`bar1`, `bar2`}

	// For valid records, we will mark as tracked only those files that have been loaded.
	s.TrackObjectPaths(record)
	assert.Equal(t, []string{`foo`, `foo/bar1`, `foo/bar2`}, s.TrackedPaths())
	assert.Equal(t, []string{`foo/bar3`}, s.UntrackedPaths())
}

func TestStateTrackRecordInvalid(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar1`, `foo`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar2`, `foo`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(`foo/bar3`, `foo`)))
	paths, err := knownpaths.New(fs)
	assert.NoError(t, err)
	s := newTestState(t, paths)

	record := &ConfigManifest{
		RecordState: RecordState{
			Persisted: true,
			Invalid:   true,
		},
	}
	record.PathInProject = NewPathInProject(``, `foo`)

	// We do not load files for invalid records.
	// Therefore, we mark all files from the object directory as tracked.
	// This will prevent duplicate error -> untracked files found.
	// The user must primarily fix why the record is invalid.
	s.TrackObjectPaths(record)
	assert.Equal(t, []string{`foo`, `foo/bar1`, `foo/bar2`, `foo/bar3`}, s.TrackedPaths())
	assert.Empty(t, s.UntrackedPaths())
}

func newTestState(t *testing.T, paths *knownpaths.Paths) *Registry {
	t.Helper()
	s := New(paths, naming.NewRegistry(), NewComponentsMap(nil), SortByPath)
	assert.NotNil(t, s)

	// Branch 1
	branch1Key := BranchKey{Id: 123}
	branch1State, err := s.CreateFrom(&BranchManifest{BranchKey: branch1Key})
	assert.NoError(t, err)
	branch1State.SetLocalState(&Branch{
		Name:      "Main",
		IsDefault: true,
	})

	// Branch 2
	branch2Key := BranchKey{Id: 567}
	branch2State, err := s.CreateFrom(&BranchManifest{BranchKey: branch2Key})
	assert.NoError(t, err)
	branch2State.SetLocalState(&Branch{
		Name:      "Foo Bar Branch",
		IsDefault: false,
	})

	// Config 1
	config1Key := ConfigKey{BranchId: 123, ComponentId: "keboola.foo", Id: `345`}
	config1State, err := s.CreateFrom(&ConfigManifest{ConfigKey: config1Key})
	assert.NoError(t, err)
	config1State.SetLocalState(&Config{
		Name: "Config 1",
	})

	// Config 2
	config2Key := ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `678`}
	config2State, err := s.GetOrCreateFrom(&ConfigManifest{ConfigKey: config2Key})
	assert.NoError(t, err)
	config2State.SetLocalState(&Config{
		Name: "Config 2",
	})

	// Config Row 1
	configRow1Key := ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `12`}
	configRow1State, err := s.CreateFrom(&ConfigRowManifest{ConfigRowKey: configRow1Key})
	assert.NoError(t, err)
	configRow1State.SetLocalState(&ConfigRow{
		Name: "Config Row 1",
	})

	// Config Row 2
	configRow2Key := ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `34`}
	configRow2State, err := s.CreateFrom(&ConfigRowManifest{ConfigRowKey: configRow2Key})
	assert.NoError(t, err)
	configRow2State.SetLocalState(&ConfigRow{
		Name: "Config Row 2",
	})

	return s
}
