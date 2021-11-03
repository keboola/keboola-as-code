package relations

import (
	"fmt"
	"strings"

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

func (m *relationsMapper) OnObjectsLoad(event model.OnObjectsLoadEvent) error {
	errors := utils.NewMultiError()

	// Link
	for _, object := range event.NewObjects {
		if o, ok := object.(model.ObjectWithRelations); ok {
			if err := m.linkRelations(o, event); err != nil {
				errors.Append(err)
			}
		}
	}

	// Validate
	for _, object := range event.NewObjects {
		if o, ok := object.(model.ObjectWithRelations); ok {
			m.validateRelations(o)
		}
	}

	return errors.ErrorOrNil()
}

// lintRelations finds the other side of the relation and create a corresponding relation on the other side.
func (m *relationsMapper) linkRelations(object model.ObjectWithRelations, event model.OnObjectsLoadEvent) error {
	errors := utils.NewMultiError()
	for _, relation := range object.GetRelations() {
		// Get relation other side
		thisSideKey := object.Key()
		otherSideKey := relation.OtherSideKey(thisSideKey)
		otherSideObject, found := event.AllObjects.Get(otherSideKey)
		if !found {
			err := fmt.Errorf(
				`%s not found, referenced from %s, by relation "%s"`,
				otherSideKey.Desc(),
				thisSideKey.Desc(),
				relation.Type(),
			)
			m.Logger.Warn(`Warning: `, err)
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

// validateRelations check relations constraints.
func (m *relationsMapper) validateRelations(object model.ObjectWithRelations) {
	allRelations := object.GetRelations()
	relationsMap := allRelations.GetAllByType()

	// Validate relations that can be defined on an object only once
	for _, t := range model.OneToXRelations() {
		if len(relationsMap[t]) > 1 {
			err := fmt.Errorf(
				`%s have %s, but only one is expected`,
				object.Desc(),
				relationsToString(t, relationsMap[t], object.Key()),
			)
			m.Logger.Warn(`Warning: `, err)

			// Remove invalid relations
			allRelations.RemoveByType(t)
		}
	}

	// Set modified relations
	object.SetRelations(allRelations)
}

// relationsToString gets string representation of the relations.
func relationsToString(relType model.RelationType, relations model.Relations, objectKey model.Key) string {
	var otherSides []string
	for _, r := range relations {
		otherSides = append(otherSides, r.OtherSideKey(objectKey).Desc())
	}
	return fmt.Sprintf(`%d relations "%s" [%s]`, len(relations), relType.String(), strings.Join(otherSides, `; `))
}
