package relations

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AfterLocalOperation links relation sides on local load.
func (m *relationsMapper) AfterLocalOperation(_ context.Context, changes *model.LocalChanges) error {
	errs := errors.NewMultiError()
	allObjects := m.state.LocalObjects()
	for _, objectState := range changes.Loaded() {
		if err := m.linkAndValidateRelations(objectState.LocalState(), allObjects); err != nil {
			errs.Append(err)
		}
	}

	// Log errors as warning
	if errs.Len() > 0 {
		m.logger.Warn(errors.PrefixError(errs, "Warning"))
	}

	return nil
}

// AfterRemoteOperation links relation sides on remote load.
func (m *relationsMapper) AfterRemoteOperation(_ context.Context, changes *model.RemoteChanges) error {
	errs := errors.NewMultiError()
	allObjects := m.state.RemoteObjects()
	for _, objectState := range changes.Loaded() {
		if err := m.linkAndValidateRelations(objectState.RemoteState(), allObjects); err != nil {
			errs.Append(err)
		}
	}

	// Log errors as warning
	if errs.Len() > 0 {
		m.logger.Warn(errors.PrefixError(errs, "Warning"))
	}

	return nil
}

func (m *relationsMapper) linkAndValidateRelations(object model.Object, allObjects model.Objects) error {
	errs := errors.NewMultiError()
	if o, ok := object.(model.ObjectWithRelations); ok {
		if err := m.linkRelations(o, allObjects); err != nil {
			errs.Append(err)
		}
		if err := m.validateRelations(o); err != nil {
			errs.Append(errors.PrefixErrorf(err, "invalid %s", object.Desc()))
		}
	}
	return errs.ErrorOrNil()
}

// lintRelations finds the other side of the relation and create a corresponding relation on the other side.
func (m *relationsMapper) linkRelations(object model.ObjectWithRelations, allObjects model.Objects) error {
	errs := errors.NewMultiError()
	relations := object.GetRelations()

	for _, relation := range relations {
		// Get other side relation
		otherSideKey, otherSideRelation, err := relation.NewOtherSideRelation(object, allObjects)
		if err != nil {
			// Remove invalid relation
			relations.Remove(relation)
			errs.Append(err)
			continue
		} else if otherSideRelation == nil {
			continue
		}

		// Get other side object
		otherSideObject, found := allObjects.Get(otherSideKey)
		if !found {
			// Remove invalid relation
			relations.Remove(relation)
			subErr := errs.AppendNested(errors.Errorf(`%s not found`, otherSideKey.Desc()))
			subErr.Append(errors.Errorf(`referenced from %s`, object.Desc()))
			subErr.Append(errors.Errorf(`by relation "%s"`, relation.Type()))
			continue
		}

		// Create and set relation to the other side
		if otherSideObject, ok := otherSideObject.(model.ObjectWithRelations); ok {
			otherSideObject.AddRelation(otherSideRelation)
		} else {
			// Remove invalid relation
			relations.Remove(relation)
			subErr := errs.AppendNested(errors.Errorf(`%s cannot have relation`, otherSideKey.Desc()))
			subErr.Append(errors.Errorf(`referenced from %s`, object.Desc()))
			subErr.Append(errors.Errorf(`by relation "%s"`, relation.Type()))
			continue
		}
	}

	object.SetRelations(relations)
	return errs.ErrorOrNil()
}

// validateRelations check relations constraints.
func (m *relationsMapper) validateRelations(object model.ObjectWithRelations) error {
	relations := object.GetRelations()
	relationsMap := relations.GetAllByType()
	errs := errors.NewMultiError()

	// Validate relations that can be defined on an object only once
	for _, t := range model.OneToXRelations() {
		if len(relationsMap[t]) > 1 {
			err := errs.AppendNested(errors.Errorf(`only one relation "%s" expected, but found %d`, t, len(relationsMap[t])))
			for _, relation := range relationsMap[t] {
				err.Append(errors.New(json.MustEncodeString(relation, false)))
			}

			// Remove invalid relations
			relations.RemoveByType(t)
		}
	}

	// Set modified relations
	object.SetRelations(relations)
	return errs.ErrorOrNil()
}
