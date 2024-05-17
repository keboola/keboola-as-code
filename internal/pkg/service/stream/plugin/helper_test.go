package plugin

import (
	"testing"
	"time"

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
	assert.False(t, isDeletedNow(now, entity))

	// Deleted now
	entity.Delete(now, by, true)
	assert.True(t, isDeletedNow(now, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isDeletedNow(now, entity))

	// Undelete now
	now = now.Add(time.Hour)
	entity.Undelete(now, by)
	assert.False(t, isDeletedNow(now, entity))

	// Undeleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isDeletedNow(now, entity))
}

func TestIsUndeletedNow(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()
	entity := &testEntity{}

	// Default
	assert.False(t, isUndeletedNow(now, entity))

	// Deleted now
	entity.Delete(now, by, true)
	assert.False(t, isUndeletedNow(now, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isUndeletedNow(now, entity))

	// Undelete now
	now = now.Add(time.Hour)
	entity.Undelete(now, by)
	assert.True(t, isUndeletedNow(now, entity))

	// Undeleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isUndeletedNow(now, entity))
}

func TestIsActivatedNow(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()
	entity := &testEntity{}

	// Default
	assert.False(t, isActivatedNow(now, entity))

	// Created now
	entity.SetCreation(now, by)
	assert.True(t, isActivatedNow(now, entity))

	// Created in the past
	now = now.Add(time.Hour)
	assert.False(t, isActivatedNow(now, entity))

	// Deleted now
	entity.Delete(now, by, true)
	assert.False(t, isActivatedNow(now, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isActivatedNow(now, entity))

	// Undeleted now
	now = now.Add(time.Hour)
	entity.Undelete(now, by)
	assert.True(t, isActivatedNow(now, entity))

	// Undeleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isActivatedNow(now, entity))

	// Disabled now
	now = now.Add(time.Hour)
	entity.Disable(now, by, "some reason", true)
	assert.False(t, isActivatedNow(now, entity))

	// Disabled in the past
	now = now.Add(time.Hour)
	assert.False(t, isActivatedNow(now, entity))

	// Enabled now
	now = now.Add(time.Hour)
	entity.Enable(now, by)
	assert.True(t, isActivatedNow(now, entity))

	// Enabled in the past
	now = now.Add(time.Hour)
	assert.False(t, isActivatedNow(now, entity))
}

func TestIsDeactivatedNow(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()
	by := test.ByUser()
	entity := &testEntity{}

	// Default
	assert.False(t, isDeactivatedNow(now, entity))

	// Created now
	entity.SetCreation(now, by)
	assert.False(t, isDeactivatedNow(now, entity))

	// Created in the past
	now = now.Add(time.Hour)
	assert.False(t, isDeactivatedNow(now, entity))

	// Deleted now
	entity.Delete(now, by, true)
	assert.True(t, isDeactivatedNow(now, entity))

	// Deleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isDeactivatedNow(now, entity))

	// Undeleted now
	now = now.Add(time.Hour)
	entity.Undelete(now, by)
	assert.False(t, isDeactivatedNow(now, entity))

	// Undeleted in the past
	now = now.Add(time.Hour)
	assert.False(t, isDeactivatedNow(now, entity))

	// Disabled now
	now = now.Add(time.Hour)
	entity.Disable(now, by, "some reason", true)
	assert.True(t, isDeactivatedNow(now, entity))

	// Disabled in the past
	now = now.Add(time.Hour)
	assert.False(t, isDeactivatedNow(now, entity))

	// Enabled now
	now = now.Add(time.Hour)
	entity.Enable(now, by)
	assert.False(t, isDeactivatedNow(now, entity))

	// Enabled in the past
	now = now.Add(time.Hour)
	assert.False(t, isDeactivatedNow(now, entity))
}
