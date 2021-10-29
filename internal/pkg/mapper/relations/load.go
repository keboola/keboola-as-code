package relations

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapAfterLocalLoad - load relations from manifest to object.
func (m *relationsMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	manifest, ok := recipe.Record.(model.ObjectManifestWithRelations)
	if !ok {
		return nil
	}

	object, ok := recipe.Object.(model.ObjectWithRelations)
	if !ok {
		return nil
	}

	object.SetRelations(manifest.GetRelations())
	return nil
}

func (m *relationsMapper) OnLoad(event model.OnObjectLoadEvent) error {
	errors := utils.NewMultiError()
	object, ok := event.Object.(model.ObjectWithRelations)
	if !ok {
		return nil
	}

	// Find the other side of the relation and create a corresponding relation on the other side
	for _, relation := range object.GetRelations() {
		// Get relation other side
		thisSideKey := object.Key()
		otherSideKey := relation.OtherSideKey(thisSideKey)
		otherSideObject, found := event.AllObjects.Get(otherSideKey)
		if !found {
			errors.Append(fmt.Errorf(
				`%s not found, referenced from %s, by relation "%s"`,
				otherSideKey.Desc(),
				thisSideKey.Desc(),
				relation.Type(),
			))
			continue
		}

		// Create and set relation to the other side
		if otherSideObject, ok := otherSideObject.(model.ObjectWithRelations); ok {
			otherSideRel := relation.NewOtherSideRelation(thisSideKey)
			if otherSideRel != nil {
				otherSideObject.AddRelation(otherSideRel)
			}
		} else {
			errors.Append(fmt.Errorf(
				`%s cannot have Relations, referenced from %s, by relation "%s"`,
				otherSideKey.Desc(),
				thisSideKey.Desc(),
				relation.Type(),
			))
		}
	}
	return errors.ErrorOrNil()
}
