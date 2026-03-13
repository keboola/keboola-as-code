package notification_test

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/notification"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestNotificationMapper_MapBeforeLocalSave(t *testing.T) {
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
	notif := &model.Notification{
		NotificationKey: notifKey,
		Event:           keboola.NotificationEventJobFailed,
		Recipient: keboola.NotificationRecipient{
			Channel: keboola.NotificationChannelEmail,
			Address: "user@example.com",
		},
		Filters: []keboola.NotificationFilter{
			{
				Field:    "job.configuration.id",
				Value:    "my-config",
				Operator: keboola.NotificationFilterOperatorEquals,
			},
		},
	}
	state := &model.NotificationState{
		NotificationManifest: manifest,
		Local:                notif,
	}

	recipe := model.NewLocalSaveRecipe(state.Manifest(), state.Local, model.NewChangedFields())
	require.NoError(t, mockedState.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	assert.Len(t, recipe.Files.All(), 2)

	configPath := mockedState.NamingGenerator().ConfigFilePath(manifest.Path())
	metaPath := mockedState.NamingGenerator().MetaFilePath(manifest.Path())

	configFile := recipe.Files.GetOneByTag(model.FileKindObjectConfig)
	require.NotNil(t, configFile)
	assert.Equal(t, configPath, configFile.Path())
	configRaw, err := configFile.ToRawFile()
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"event": "job-failed",
		"filters": [{"field": "job.configuration.id", "value": "my-config", "operator": "=="}],
		"recipient": {"channel": "email", "address": "user@example.com"}
	}`, configRaw.Content)

	metaFile := recipe.Files.GetOneByTag(model.FileKindObjectMeta)
	require.NotNil(t, metaFile)
	assert.Equal(t, metaPath, metaFile.Path())
	metaRaw, err := metaFile.ToRawFile()
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, metaRaw.Content)
}
