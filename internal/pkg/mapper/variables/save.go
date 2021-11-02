package variables

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *variablesMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Variables are used by config
	internalObject, ok := recipe.InternalObject.(*model.Config)
	if !ok {
		return nil
	}

	// Get relation
	relType := model.VariablesFromRelType
	relations := internalObject.Relations.GetByType(relType)
	if len(relations) == 0 {
		return nil
	} else if len(relations) > 1 {
		return fmt.Errorf(
			`unexpected state: %s has %d relations of the "%s" type, only one allowed`,
			recipe.Manifest.Desc(),
			len(relations),
			relType,
		)
	}

	// Set variables ID
	relation := relations[0].(*model.VariablesFromRelation)
	apiObject := recipe.ApiObject.(*model.Config)
	apiObject.Content.Set(model.VariablesIdContentKey, relation.Source.Id)

	// Delete relation
	apiObject.Relations.RemoveByType(relType)

	return nil
}
