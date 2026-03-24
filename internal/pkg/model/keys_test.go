package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotificationKey_Kind(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	kind := key.Kind()
	assert.Equal(t, NotificationKind, kind.Name)
	assert.Equal(t, NotificationAbbr, kind.Abbr)
}

func TestNotificationKey_ObjectID(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	assert.Equal(t, "notif-123", key.ObjectID())
}

func TestNotificationKey_Level(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	assert.Equal(t, 4, key.Level())
}

func TestNotificationKey_Key(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	assert.Equal(t, key, key.Key())
}

func TestNotificationKey_Desc(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	expected := `notification "branch:123/component:keboola.orchestrator/config:456/notification:notif-123"`
	assert.Equal(t, expected, key.Desc())
}

func TestNotificationKey_String(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	expected := "04_123_keboola.orchestrator_456_notif-123_notification"
	assert.Equal(t, expected, key.String())
}

func TestNotificationKey_ConfigKey(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	expected := ConfigKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ID:          "456",
	}
	assert.Equal(t, expected, key.ConfigKey())
}

func TestNotificationKey_ParentKey(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	expected := ConfigKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ID:          "456",
	}
	parent, err := key.ParentKey()
	require.NoError(t, err)
	assert.Equal(t, expected, parent)
}

func TestNotificationKey_ImplementsKeyInterface(t *testing.T) {
	t.Parallel()

	key := NotificationKey{
		BranchID:    123,
		ComponentID: "keboola.orchestrator",
		ConfigID:    "456",
		ID:          "notif-123",
	}

	// Verify that NotificationKey implements the Key interface
	var _ Key = key
}
