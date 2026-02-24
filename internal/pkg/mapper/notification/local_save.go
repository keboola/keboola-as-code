package notification

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave creates notification subdirectory and meta.json file.
func (m *mapper) MapBeforeLocalSave(_ context.Context, recipe *model.LocalSaveRecipe) error {
	notification, ok := recipe.Object.(*model.Notification)
	if !ok {
		return nil
	}

	manifest, ok := recipe.ObjectManifest.(*model.NotificationManifest)
	if !ok {
		return nil
	}

	// Set relative path for notification
	manifest.SetRelativePath(filesystem.Join("notifications", string(notification.ID)))

	// Create meta.json with notification details
	metaFile := filesystem.Join(recipe.Path(), "meta.json")
	metaContent := orderedmap.New()
	metaContent.Set("event", notification.Event)
	metaContent.Set("recipient", notification.Recipient)
	if len(notification.Filters) > 0 {
		metaContent.Set("filters", notification.Filters)
	}
	if notification.ExpiresAt != nil {
		metaContent.Set("expiresAt", notification.ExpiresAt)
	}

	jsonFile := filesystem.NewJSONFile(metaFile, metaContent)
	recipe.Files.
		Add(jsonFile).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectMeta)

	return nil
}
