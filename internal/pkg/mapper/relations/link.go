package relations

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AfterLocalOperation links relation sides on local load.
func (m *relationsMapper) AfterLocalOperation(ctx context.Context, changes *model.LocalChanges) error {
	errs := errors.NewMultiError()
	allObjects := m.state.LocalObjects()
	loaded := changes.Loaded()

	// Pass 1: link all objects so every other-side relation exists before validation.
	// Processing link and validate in a single loop would cause order-dependent behaviour:
	// a variables config iterated before its consumers would be validated before the consumer
	// linking steps add VariablesForRelation entries to it, leaving duplicates undetected until
	// PathsGenerator runs and hits a fatal "multiple parents" error.
	for _, objectState := range loaded {
		if o, ok := objectState.LocalState().(model.ObjectWithRelations); ok {
			if err := m.linkRelations(o, allObjects); err != nil {
				errs.Append(err)
			}
		}
	}

	// Pass 2: validate all objects now that the relation graph is complete.
	for _, objectState := range loaded {
		if o, ok := objectState.LocalState().(model.ObjectWithRelations); ok {
			if err := m.validateRelations(o); err != nil {
				errs.Append(errors.PrefixErrorf(err, "invalid %s", objectState.LocalState().Desc()))
			}
		}
	}

	// Log errors as warning
	if errs.Len() > 0 {
		m.logger.Warn(ctx, errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
	}

	return nil
}

// AfterRemoteOperation links relation sides on remote load.
func (m *relationsMapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	errs := errors.NewMultiError()
	allObjects := m.state.RemoteObjects()
	loaded := changes.Loaded()

	// Pass 1: link all objects so every other-side relation exists before validation.
	// See AfterLocalOperation for the reasoning behind the two-pass approach.
	for _, objectState := range loaded {
		if o, ok := objectState.RemoteState().(model.ObjectWithRelations); ok {
			if err := m.linkRelations(o, allObjects); err != nil {
				errs.Append(err)
			}
		}
	}

	// Pass 2: validate all objects now that the relation graph is complete.
	for _, objectState := range loaded {
		if o, ok := objectState.RemoteState().(model.ObjectWithRelations); ok {
			if err := m.validateRelations(o); err != nil {
				errs.Append(errors.PrefixErrorf(err, "invalid %s", objectState.RemoteState().Desc()))
			}
		}
	}

	// Log errors as warning
	if errs.Len() > 0 {
		m.logger.Warn(ctx, errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
	}

	return nil
}

// linkRelations finds the other side of the relation and create a corresponding relation on the other side.
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
