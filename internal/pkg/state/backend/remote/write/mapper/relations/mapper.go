package relations

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type mapper struct {
	dependencies
}

type dependencies interface {
}

func NewMapper() *mapper {
	return &mapper{}
}

// MapBeforeRemoteSave - modify changed fields.
func (m *mapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	if recipe.ChangedFields.Has(`relations`) {
		// Relations are stored on the API side in config/row configuration
		recipe.ChangedFields.Add(`configuration`)
		recipe.ChangedFields.Remove(`relations`)
	}
	return nil
}
