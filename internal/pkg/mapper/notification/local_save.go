package notification

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapBeforeLocalSave writes notification config.json and meta.json files.
func (m *mapper) MapBeforeLocalSave(_ context.Context, recipe *model.LocalSaveRecipe) error {
	notification, ok := recipe.Object.(*model.Notification)
	if !ok {
		return nil
	}

	if err := m.saveConfigFile(recipe, notification); err != nil {
		return err
	}
	m.saveMetaFile(recipe)
	return nil
}

func (m *mapper) saveConfigFile(recipe *model.LocalSaveRecipe, notification *model.Notification) error {
	type configContent struct {
		Event     keboola.NotificationEvent       `json:"event"`
		Filters   []keboola.NotificationFilter    `json:"filters,omitempty"`
		Recipient keboola.NotificationRecipient   `json:"recipient"`
		ExpiresAt *keboola.NotificationExpiration `json:"expiresAt,omitempty"`
	}

	content := configContent{
		Event:     notification.Event,
		Filters:   notification.Filters,
		Recipient: notification.Recipient,
		ExpiresAt: notification.ExpiresAt,
	}

	om := orderedmap.New()
	if err := json.ConvertByJSON(content, om); err != nil {
		return errors.Errorf("failed to convert notification to orderedmap: %w", err)
	}

	path := m.state.NamingGenerator().ConfigFilePath(recipe.Path())
	recipe.Files.
		Add(filesystem.NewJSONFile(path, om)).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectConfig)
	return nil
}

func (m *mapper) saveMetaFile(recipe *model.LocalSaveRecipe) {
	path := m.state.NamingGenerator().MetaFilePath(recipe.Path())
	recipe.Files.
		Add(filesystem.NewJSONFile(path, orderedmap.New())).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectMeta)
}
