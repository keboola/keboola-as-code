package notification_test

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/notification"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestNotificationMapper_MapAfterLocalLoad(t *testing.T) {
	t.Parallel()
	d := dependencies.NewMocked(t, t.Context())
	logger := d.DebugLogger()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(notification.NewMapper(mockedState))

	notifKey := model.NotificationKey{
		BranchID:    123,
		ComponentID: keboola.ComponentID("ex-generic-v2"),
		ConfigID:    keboola.ConfigID("my-config"),
		ID:          keboola.NotificationSubscriptionID("sub-123"),
	}
	manifest := &model.NotificationManifest{
		NotificationKey: notifKey,
		Paths: model.Paths{
			AbsPath: model.NewAbsPath("main/extractor/ex-generic-v2/my-config", "notifications/sub-123"),
		},
	}

	// Write config.json and meta.json to the mocked FS.
	fs := mockedState.ObjectsRoot()
	ctx := t.Context()
	require.NoError(t, fs.Mkdir(ctx, manifest.Path()))
	configJSON := `{
		"event": "job-failed",
		"filters": [{"field": "job.configuration.id", "value": "my-config", "operator": "=="}],
		"recipient": {"channel": "email", "address": "user@example.com"}
	}`
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(
		mockedState.NamingGenerator().ConfigFilePath(manifest.Path()),
		configJSON,
	)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(
		mockedState.NamingGenerator().MetaFilePath(manifest.Path()),
		`{}`,
	)))

	notif := &model.Notification{NotificationKey: notifKey}
	recipe := model.NewLocalLoadRecipe(mockedState.FileLoader(), manifest, notif)
	require.NoError(t, mockedState.Mapper().MapAfterLocalLoad(ctx, recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	assert.Equal(t, keboola.NotificationEventJobFailed, notif.Event)
	assert.Equal(t, keboola.NotificationRecipient{
		Channel: keboola.NotificationChannelEmail,
		Address: "user@example.com",
	}, notif.Recipient)
	require.Len(t, notif.Filters, 1)
	assert.Equal(t, keboola.NotificationFilter{
		Field:    "job.configuration.id",
		Value:    "my-config",
		Operator: keboola.NotificationFilterOperatorEquals,
	}, notif.Filters[0])
}
