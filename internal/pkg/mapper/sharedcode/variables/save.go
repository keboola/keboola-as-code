package variables

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave - add "variables_id" to shared code.
func (m *mapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Variables are used by shared code - config row.
	internalObject, ok := recipe.InternalObject.(*model.ConfigRow)
	if !ok {
		return nil
	}
	apiObject := recipe.ApiObject.(*model.ConfigRow)

	// Check component type
	component, err := m.State.Components().Get(internalObject.ComponentKey())
	if err != nil {
		return err
	}
	if !component.IsSharedCode() {
		return nil
	}

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
	apiObject.Content.Set(model.SharedCodeVariablesIdContentKey, relation.VariablesId)

	// Delete relation
	apiObject.Relations.RemoveByType(relType)
	return nil
}
