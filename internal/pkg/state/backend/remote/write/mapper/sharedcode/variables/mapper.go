package variables

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapBeforeRemoteSave - add "variables_id" to shared code.
func (m *mapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	if ok, err := m.IsSharedCodeRowKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}
	object := recipe.Object.(*model.ConfigRow)

	// Get relation
	relType := model.SharedCodeVariablesFromRelType
	relationRaw, err := object.Relations.GetOneByType(relType)
	if err != nil {
		return errors.Errorf(`unexpected state of %s: %w`, recipe.Desc(), err)
	} else if relationRaw == nil {
		return nil
	}
	relation := relationRaw.(*model.SharedCodeVariablesFromRelation)

	// Set variables ID
	object.Content.Set(model.SharedCodeVariablesIdContentKey, relation.VariablesId.String())

	// Delete relation
	object.Relations.RemoveByType(relType)
	return nil
}
