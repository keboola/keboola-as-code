package service

import (
	"context"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func validateInputs(ctx context.Context, backends []string, groups template.StepsGroups, payload []*StepPayload) (out *ValidationResult, allValues template.InputsValues, err error) {
	out = &ValidationResult{Valid: true}
	stepInputs := inputsPayloadToMap(payload)

	errs := errors.NewMultiError()
	allValues = make(template.InputsValues, 0)
	allValuesMap := make(map[string]any)
	allStepsIds := make(map[string]bool)

	// Check each group
	for _, group := range groups.ToExtended() {
		outGroup := &StepGroupValidationResult{ID: group.ID, Valid: true, Steps: make([]*StepValidationResult, 0)}
		out.StepGroups = append(out.StepGroups, outGroup)
		configuredSteps := 0
		stepsCount := 0
		// Check each step
		for _, step := range group.Steps {
			var filteredInputs int
			for _, input := range step.Inputs {
				if !input.MatchesAvailableBackend(backends) {
					filteredInputs++
					continue
				}
			}

			if filteredInputs != 0 {
				continue
			}
			outStep := &StepValidationResult{ID: step.ID, Valid: true, Inputs: make([]*InputValidationResult, 0)}
			outGroup.Steps = append(outGroup.Steps, outStep)
			allStepsIds[step.ID] = true
			stepInputsIds := make(map[string]bool)

			// Get values in step
			values, stepFound := stepInputs[step.ID]
			if stepFound || len(step.Inputs) == 0 {
				// Step is configured, if it is part of the payload,
				// or there are no inputs in the step.
				outStep.Configured = true
				configuredSteps++
			}

			// Check each input
			for _, input := range step.Inputs {
				outInput := &InputValidationResult{ID: input.ID}
				outStep.Inputs = append(outStep.Inputs, outInput)
				stepInputsIds[input.ID] = true

				// Is input available/visible?
				if v, err := input.Available(allValuesMap); err != nil {
					errs.Append(errors.Errorf(`cannot evaluate "showIf" condition for input "%s": %w`, input.ID, err))
				} else {
					outInput.Visible = v
				}

				// Get value
				value, found := values[input.ID]
				if !found || !outInput.Visible {
					value = input.Empty()
				}

				// Validate value
				if outInput.Error == nil && outStep.Configured && outInput.Visible {
					if err := input.ValidateUserInput(ctx, value); err != nil {
						msg := err.Error()

						// In other parts of the repository, the validation result is a bullet list.
						// But there is always only one message, so in the API it is formatted as a sentence.
						msg = strhelper.AsSentence(msg)

						outInput.Error = &msg
					}
				}

				// Add value to context
				allValuesMap[input.ID] = value
				allValues = append(allValues, template.InputValue{
					ID:      input.ID,
					Value:   value,
					Skipped: !outInput.Visible || !outStep.Configured,
				})

				// Propagate invalid state from input to step
				if outInput.Error != nil {
					outStep.Valid = false
				}
			}

			// Check unexpected inputs in the step payload
			for inputID := range stepInputs[step.ID] {
				if !stepInputsIds[inputID] {
					errs.Append(errors.Errorf(`found unexpected input "%s" in step "%s"`, inputID, step.ID))
				}
			}

			// Propagate invalid state from step to group
			if !outStep.Valid {
				outGroup.Valid = false
			}
			stepsCount++
		}

		// Check if required number of steps is configured
		if err := group.ValidateStepsCount(stepsCount, configuredSteps); err != nil {
			msg := strhelper.AsSentence(err.Error())
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
		return nil, nil, NewBadRequestError(errors.PrefixError(errs, "invalid payload"))
	}

	return out, allValues, nil
}

// inputsPayloadToMap returns map[StepId][InputId] -> value.
func inputsPayloadToMap(payload []*StepPayload) map[string]map[string]any {
	v := make(map[string]map[string]any)
	for _, stepPayload := range payload {
		if _, ok := v[stepPayload.ID]; !ok {
			v[stepPayload.ID] = make(map[string]any)
		}
		for _, inputPayload := range stepPayload.Inputs {
			v[stepPayload.ID][inputPayload.ID] = inputPayload.Value
		}
	}
	return v
}
