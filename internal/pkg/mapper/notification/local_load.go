package notification

import (
	"context"
	"path/filepath"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapAfterLocalLoad loads notifications from notifications/ subdirectories.
func (m *mapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	configManifest, ok := recipe.ObjectManifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	fs := m.state.LocalManager().Fs()
	configDir := configManifest.Path()
	notificationsDir := filesystem.Join(configDir, "notifications")

	if !fs.IsDir(ctx, notificationsDir) {
		return nil
	}

	entries, err := fs.ReadDir(ctx, notificationsDir)
	if err != nil {
		return errors.Errorf(`cannot read notifications directory "%s": %w`, notificationsDir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		subscriptionID := keboola.NotificationSubscriptionID(entry.Name())
		metaFile := filesystem.Join(notificationsDir, entry.Name(), "meta.json")

		var metaContent struct {
			Event     keboola.NotificationEvent       `json:"event"`
			Filters   []keboola.NotificationFilter    `json:"filters,omitempty"`
			Recipient keboola.NotificationRecipient   `json:"recipient"`
			ExpiresAt *keboola.NotificationExpiration `json:"expiresAt,omitempty"`
		}

		file, err := fs.ReadFile(ctx, filesystem.NewFileDef(metaFile).SetDescription("notification meta"))
		if err != nil {
			m.logger.Warnf(ctx, `cannot read notification meta file "%s": %s`, metaFile, err)
			continue
		}

		if err := json.DecodeString(file.Content, &metaContent); err != nil {
			m.logger.Warnf(ctx, `cannot decode notification meta file "%s": %s`, metaFile, err)
			continue
		}

		for _, filter := range metaContent.Filters {
			if err := filter.Validate(); err != nil {
				return errors.Errorf(`invalid filter in notification "%s": %w`, subscriptionID, err)
			}
		}

		notification := &model.Notification{
			NotificationKey: model.NotificationKey{
				BranchID:    configManifest.BranchID,
				ComponentID: configManifest.ComponentID,
				ConfigID:    configManifest.ID,
				ID:          subscriptionID,
			},
			Event:     metaContent.Event,
			Filters:   metaContent.Filters,
			Recipient: metaContent.Recipient,
			ExpiresAt: metaContent.ExpiresAt,
		}

		notificationManifest := &model.NotificationManifest{
			NotificationKey: notification.NotificationKey,
		}
		notificationManifest.SetParentPath(configDir)
		notificationManifest.SetRelativePath(filepath.Join("notifications", entry.Name()))

		notificationState := &model.NotificationState{
			NotificationManifest: notificationManifest,
			Local:                notification,
		}

		if err := m.state.Set(notificationState); err != nil {
			return err
		}
	}

	return nil
}
