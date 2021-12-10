package variables

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave - add "variables_id" to shared code.
func (m *mapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	if ok, err := m.IsSharedCodeRowKey(recipe.InternalObject.Key()); err != nil || !ok {
		return err
	}
	apiObject := recipe.ApiObject.(*model.ConfigRow)
	internalObject := recipe.InternalObject.(*model.ConfigRow)

	// Get relation
	relType := model.SharedCodeVariablesFromRelType
	relationRaw, err := internalObject.Relations.GetOneByType(relType)
	if err != nil {
		return fmt.Errorf(`unexpected state of %s: %w`, recipe.Manifest.Desc(), err)
	} else if relationRaw == nil {
		return nil
	}
	relation := relationRaw.(*model.SharedCodeVariablesFromRelation)

	// Set variables ID
	apiObject.Content.Set(model.SharedCodeVariablesIdContentKey, relation.VariablesId.String())

	// Delete relation
	apiObject.Relations.RemoveByType(relType)
	return nil
}
