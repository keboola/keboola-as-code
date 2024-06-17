package plugin

import (
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

type testEntity struct {
	definition.Created
	definition.Versioned
	definition.Switchable
	definition.SoftDeletable
}

func TestIsDeletedNow(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()
	entity := &testEntity{}

	// Default
	original := (*testEntity)(nil)
	assert.False(t, isDeletedNow(original, entity))

	// Deleted now
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Delete(now, by, true)
	assert.True(t, isDeletedNow(original, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isDeletedNow(original, entity))

	// Undelete now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Undelete(now, by)
	assert.False(t, isDeletedNow(original, entity))

	// Undeleted in the past
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isDeletedNow(original, entity))
}

func TestIsUndeletedNow(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()
	entity := &testEntity{}

	// Default
	original := (*testEntity)(nil)
	assert.False(t, isUndeletedNow(original, entity))

	// Deleted now
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Delete(now, by, true)
	assert.False(t, isUndeletedNow(original, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isUndeletedNow(original, entity))

	// Undelete now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Undelete(now, by)
	assert.True(t, isUndeletedNow(original, entity))

	// Undeleted in the past
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isUndeletedNow(original, entity))
}

func TestIsActivatedNow(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()
	entity := &testEntity{}

	// Created now
	original := (*testEntity)(nil)
	assert.True(t, isActivatedNow(original, entity))

	// Created in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isActivatedNow(original, entity))

	// Deleted now
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Delete(now, by, true)
	assert.False(t, isActivatedNow(original, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isActivatedNow(original, entity))

	// Undeleted now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Undelete(now, by)
	assert.True(t, isActivatedNow(original, entity))

	// Undeleted in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isActivatedNow(original, entity))

	// Disabled now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Disable(now, by, "some reason", true)
	assert.False(t, isActivatedNow(original, entity))

	// Disabled in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isActivatedNow(original, entity))

	// Enabled now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Enable(now, by)
	assert.True(t, isActivatedNow(original, entity))

	// Enabled in the past
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isActivatedNow(original, entity))
}

func TestIsDeactivatedNow(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()
	entity := &testEntity{}

	// Default
	original := (*testEntity)(nil)
	assert.False(t, isDeactivatedNow(original, entity))

	// Created now
	original = deepcopy.Copy(entity).(*testEntity)
	entity.SetCreation(now, by)
	assert.False(t, isDeactivatedNow(original, entity))

	// Created in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isDeactivatedNow(original, entity))

	// Deleted now
	entity.Delete(now, by, true)
	assert.True(t, isDeactivatedNow(original, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isDeactivatedNow(original, entity))

	// Undeleted now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Undelete(now, by)
	assert.False(t, isDeactivatedNow(original, entity))

	// Undeleted in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isDeactivatedNow(original, entity))

	// Disabled now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Disable(now, by, "some reason", true)
	assert.True(t, isDeactivatedNow(original, entity))

	// Disabled in the past
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isDeactivatedNow(original, entity))

	// Enabled now
	now = now.Add(time.Hour)
	original = deepcopy.Copy(entity).(*testEntity)
	entity.Enable(now, by)
	assert.False(t, isDeactivatedNow(original, entity))

	// Enabled in the past
	original = deepcopy.Copy(entity).(*testEntity)
	assert.False(t, isDeactivatedNow(original, entity))
}
