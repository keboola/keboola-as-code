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
