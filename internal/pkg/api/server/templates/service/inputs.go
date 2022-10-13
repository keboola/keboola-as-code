package service

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func validateInputs(groups template.StepsGroups, payload []*StepPayload) (out *ValidationResult, allValues template.InputsValues, err error) {
	out = &ValidationResult{Valid: true}
	errFormatter := NewValidationErrorFormatter()
	stepInputs := inputsPayloadToMap(payload)

	errs := errors.NewMultiError()
	allValues = make(template.InputsValues, 0)
	allValuesMap := make(map[string]interface{})
	allStepsIds := make(map[string]bool)

	// Check each group
	for _, group := range groups.ToExtended() {
		outGroup := &StepGroupValidationResult{ID: group.Id, Valid: true, Steps: make([]*StepValidationResult, 0)}
		out.StepGroups = append(out.StepGroups, outGroup)
		configuredSteps := 0

		// Check each step
		for _, step := range group.Steps {
			outStep := &StepValidationResult{ID: step.Id, Valid: true, Inputs: make([]*InputValidationResult, 0)}
			outGroup.Steps = append(outGroup.Steps, outStep)
			allStepsIds[step.Id] = true
			stepInputsIds := make(map[string]bool)

			// Get values in step
			values, stepFound := stepInputs[step.Id]
			if stepFound || len(step.Inputs) == 0 {
				// Step is configured, if it is part of the payload,
				// or there are no inputs in the step.
				outStep.Configured = true
				configuredSteps++
			}

			// Check each input
			for _, input := range step.Inputs {
				outInput := &InputValidationResult{ID: input.Id}
				outStep.Inputs = append(outStep.Inputs, outInput)
				stepInputsIds[input.Id] = true

				// Is input available/visible?
				if v, err := input.Available(allValuesMap); err != nil {
					errs.Append(errors.Errorf(`cannot evaluate "showIf" condition for input "%s": %w`, input.Id, err))
				} else {
					outInput.Visible = v
				}

				// Get value
				value, found := values[input.Id]
				if !found || !outInput.Visible {
					value = input.Empty()
				}

				// Validate value
				if outInput.Error == nil && outStep.Configured && outInput.Visible {
					if err := input.ValidateUserInput(value); err != nil {
						msg := errFormatter.Format(err)
						outInput.Error = &msg
					}
				}

				// Add value to context
				allValuesMap[input.Id] = value
				allValues = append(allValues, template.InputValue{
					Id:      input.Id,
					Value:   value,
					Skipped: !outInput.Visible || !outStep.Configured,
				})

				// Propagate invalid state from input to step
				if outInput.Error != nil {
					outStep.Valid = false
				}
			}

			// Check unexpected inputs in the step payload
			for inputId := range stepInputs[step.Id] {
				if !stepInputsIds[inputId] {
					errs.Append(errors.Errorf(`found unexpected input "%s" in step "%s"`, inputId, step.Id))
				}
			}

			// Propagate invalid state from step to group
			if !outStep.Valid {
				outGroup.Valid = false
			}
		}

		// Check if required number of steps is configured
		if err := group.ValidateStepsCount(len(group.Steps), configuredSteps); err != nil {
			msg := errFormatter.Format(err)
			outGroup.Error = &msg
			outGroup.Valid = false
		}

		// Propagate invalid state from group to result
		if !outGroup.Valid {
			out.Valid = false
		}
	}

	// Check unexpected steps in payload
	for _, step := range payload {
		if !allStepsIds[step.ID] {
			errs.Append(errors.Errorf(`found unexpected step "%s"`, step.ID))
			continue
		}
	}

	// Format payload errors
	if errs.Len() > 0 {
		return nil, nil, BadRequestError{
			Message: errFormatter.Format(errors.PrefixError(errs, "Invalid payload")),
		}
	}

	return out, allValues, nil
}

// inputsPayloadToMap returns map[StepId][InputId] -> value.
func inputsPayloadToMap(payload []*StepPayload) map[string]map[string]interface{} {
	v := make(map[string]map[string]interface{})
	for _, stepPayload := range payload {
		if _, ok := v[stepPayload.ID]; !ok {
			v[stepPayload.ID] = make(map[string]interface{})
		}
		for _, inputPayload := range stepPayload.Inputs {
			v[stepPayload.ID][inputPayload.ID] = inputPayload.Value
		}
	}
	return v
}
