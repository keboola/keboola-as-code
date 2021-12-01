package relations

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// OnObjectsLoad links relation sides on remote and  local load.
func (m *relationsMapper) OnObjectsLoad(event model.OnObjectsLoadEvent) error {
	errors := utils.NewMultiError()

	// Link and validate
	for _, object := range event.NewObjects {
		if o, ok := object.(model.ObjectWithRelations); ok {
			if err := m.linkRelations(o, event); err != nil {
				errors.Append(err)
			}
			if err := m.validateRelations(o); err != nil {
				errors.Append(utils.PrefixError(fmt.Sprintf(`invalid %s`, object.Desc()), err))
			}
		}
	}

	// Log errors as warning
	if errors.Len() > 0 {
		m.Logger.Warn(utils.PrefixError(`Warning`, errors))
	}

	return nil
}

// lintRelations finds the other side of the relation and create a corresponding relation on the other side.
func (m *relationsMapper) linkRelations(object model.ObjectWithRelations, event model.OnObjectsLoadEvent) error {
	errors := utils.NewMultiError()
	relations := object.GetRelations()

	for _, relation := range relations {
		// Get other side relation
		otherSideKey, otherSideRelation, err := relation.NewOtherSideRelation(object, event.AllObjects)
		if err != nil {
			// Remove invalid relation
			relations.Remove(relation)
			errors.Append(err)
			continue
		} else if otherSideRelation == nil {
			continue
		}

		// Get other side object
		otherSideObject, found := event.AllObjects.Get(otherSideKey)
		if !found {
			// Remove invalid relation
			relations.Remove(relation)
			errors.Append(fmt.Errorf(`%s not found`, otherSideKey.Desc()))
			errors.Append(fmt.Errorf(`  - referenced from %s`, object.Desc()))
			errors.Append(fmt.Errorf(`  - by relation "%s"`, relation.Type()))
			continue
		}

		// Create and set relation to the other side
		if otherSideObject, ok := otherSideObject.(model.ObjectWithRelations); ok {
			otherSideObject.AddRelation(otherSideRelation)
		} else {
			// Remove invalid relation
			relations.Remove(relation)
			errors.Append(fmt.Errorf(`%s cannot have relation`, otherSideKey.Desc()))
			errors.Append(fmt.Errorf(`  - referenced from %s`, object.Desc()))
			errors.Append(fmt.Errorf(`  - by relation "%s"`, relation.Type()))
			continue
		}
	}

	object.SetRelations(relations)
	return errors.ErrorOrNil()
}

// validateRelations check relations constraints.
func (m *relationsMapper) validateRelations(object model.ObjectWithRelations) error {
	relations := object.GetRelations()
	relationsMap := relations.GetAllByType()
	errors := utils.NewMultiError()

	// Validate relations that can be defined on an object only once
	for _, t := range model.OneToXRelations() {
		if len(relationsMap[t]) > 1 {
			errors.Append(fmt.Errorf(`only one relation "%s" expected, but found %d`, t, len(relationsMap[t])))
			for _, relation := range relationsMap[t] {
				errors.Append(fmt.Errorf(`  - %s`, json.MustEncodeString(relation, false)))
			}

			// Remove invalid relations
			relations.RemoveByType(t)
		}
	}

	// Set modified relations
	object.SetRelations(relations)
	return errors.ErrorOrNil()
}
