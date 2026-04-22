package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestManifestRecordGetParent(t *testing.T) {
	t.Parallel()
	r := NewRecords(model.SortByID)
	branchManifest := &model.BranchManifest{BranchKey: model.BranchKey{ID: 123}}
	configManifest := &model.ConfigManifest{ConfigKey: model.ConfigKey{
		BranchID:    123,
		ComponentID: "keboola.foo",
		ID:          "456",
	}}
	require.NoError(t, r.trackRecord(branchManifest))
	parent, err := r.GetParent(configManifest)
	assert.Equal(t, branchManifest, parent)
	require.NoError(t, err)
}

func TestManifestRecordGetParentNotFound(t *testing.T) {
	t.Parallel()
	r := NewRecords(model.SortByID)
	configManifest := &model.ConfigManifest{ConfigKey: model.ConfigKey{
		BranchID:    123,
		ComponentID: "keboola.foo",
		ID:          "456",
	}}
	parent, err := r.GetParent(configManifest)
	assert.Nil(t, parent)
	require.Error(t, err)
	assert.Equal(t, `manifest record for branch "123" not found, referenced from config "branch:123/component:keboola.foo/config:456"`, err.Error())
}

func TestManifestRecordGetParentNil(t *testing.T) {
	t.Parallel()
	r := NewRecords(model.SortByID)
	parent, err := r.GetParent(&model.BranchManifest{})
	assert.Nil(t, parent)
	require.NoError(t, err)
}

// TestSetRecords_OrphanedScheduler_SiblingGetsPathResolved is a regression test
// for the panic in AddRelatedPath that occurred when a manifest contained both an
// orphaned scheduler and sibling configs that sorted after it.
//
// The pre-fix code did an early return from SetRecords on the first PersistRecord
// failure. Any record that had not yet been processed was left with
// parentPathSet=false. When a subsequent pull tried to write those records' files,
// Path() returned only the bare relative path (without the branch prefix), so the
// filesystem.IsFrom check in AddRelatedPath panicked.
//
// The fix continues processing all records and deletes only the failing ones, so
// every surviving record has its parent path resolved before SetRecords returns.
func TestSetRecords_OrphanedScheduler_SiblingGetsPathResolved(t *testing.T) {
	t.Parallel()
	r := NewRecords(model.SortByID)

	branchManifest := &model.BranchManifest{
		BranchKey: model.BranchKey{ID: 1},
		// Simulate JSON-deserialized state: only RelativePath is set,
		// parentPath and parentPathSet are zero values.
		Paths: model.Paths{AbsPath: model.AbsPath{RelativePath: "main"}},
	}
	// Scheduler whose target orchestrator is absent from the manifest.
	// Placed before the extractor so that the pre-fix early-return would leave
	// the extractor unprocessed (parentPathSet=false).
	schedulerManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchID:    1,
			ComponentID: "keboola.scheduler",
			ID:          "456",
		},
		Paths: model.Paths{AbsPath: model.AbsPath{RelativePath: "schedules/scheduler"}},
		Relations: model.Relations{
			&model.SchedulerForRelation{
				ComponentID: "keboola.orchestrator",
				ConfigID:    "999", // deliberately absent from the manifest
			},
		},
	}
	// Sibling extractor with no special relations — its parent is the branch.
	// After SetRecords it must have IsParentPathSet()==true and the full path
	// "main/extractor/ex-generic-v2/empty" (branch prefix included).
	// With the pre-fix code this record would have parentPathSet=false, causing
	// a panic in AddRelatedPath when the pull wrote meta.json under the config.
	extractorManifest := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchID:    1,
			ComponentID: "ex-generic-v2",
			ID:          "789",
		},
		Paths: model.Paths{AbsPath: model.AbsPath{RelativePath: "extractor/ex-generic-v2/empty"}},
	}

	err := r.SetRecords([]model.ObjectManifest{branchManifest, schedulerManifest, extractorManifest})

	// The orphaned scheduler is reported but does not hard-fail the whole load.
	require.Error(t, err)
	assert.Contains(t, err.Error(), `manifest record for config "branch:1/component:keboola.orchestrator/config:999" not found`)

	// Orphaned scheduler must be deleted.
	_, found := r.GetRecord(schedulerManifest.Key())
	assert.False(t, found, "orphaned scheduler must be removed from records")

	// Branch must survive and be resolved.
	branchRecord, found := r.GetRecord(branchManifest.Key())
	require.True(t, found, "branch record must remain")
	assert.True(t, branchRecord.IsParentPathSet())

	// Sibling extractor must survive with its parent path correctly resolved so
	// that Path() returns "main/extractor/ex-generic-v2/empty" and not the bare
	// relative path "extractor/ex-generic-v2/empty" (which would panic later).
	extRecord, found := r.GetRecord(extractorManifest.Key())
	require.True(t, found, "sibling extractor must not be removed")
	assert.True(t, extRecord.IsParentPathSet(),
		"sibling extractor must have parent path resolved even after the orphaned scheduler fails")
	assert.Equal(t, "main/extractor/ex-generic-v2/empty", extRecord.Path())
}
