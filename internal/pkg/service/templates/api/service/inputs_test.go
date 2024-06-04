package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

type validateCase struct {
	name string
	// Input
	groups  template.StepsGroups
	payload []*StepPayload
	// Expected
	result *ValidationResult
	values template.InputsValues
	err    string
}

func TestValidateInputs(t *testing.T) {
	t.Parallel()

	cases := []validateCase{
		// One group, with one step, with one input
		{
			name: "simple",
			groups: template.StepsGroups{
				{
					Required: input.RequiredAtLeastOne,
					Steps: template.Steps{
						{
							Inputs: template.Inputs{
								{ID: "foo", Type: input.TypeString, Kind: input.KindInput, Rules: "required"},
							},
						},
					},
				},
			},
			payload: []*StepPayload{{ID: "g01-s01", Inputs: []*InputValue{{ID: "foo", Value: "bar"}}}},
			result: &ValidationResult{
				Valid: true,
				StepGroups: []*StepGroupValidationResult{
					{
						ID:    "g01",
						Valid: true,
						Steps: []*StepValidationResult{
							{
								ID:         "g01-s01",
								Configured: true,
								Valid:      true,
								Inputs: []*InputValidationResult{
									{
										ID:      "foo",
										Visible: true,
									},
								},
							},
						},
					},
				},
			},
			values: input.Values{{ID: "foo", Value: "bar", Skipped: false}},
		},
		// Optional group, step is configured
		{
			name: "optional-group-configured",
			groups: template.StepsGroups{
				{
					Required: input.RequiredOptional,
					Steps: template.Steps{
						{
							Inputs: template.Inputs{
								{ID: "foo", Type: input.TypeString, Kind: input.KindInput, Rules: "required"},
							},
						},
					},
				},
			},
			payload: []*StepPayload{{ID: "g01-s01", Inputs: []*InputValue{{ID: "foo", Value: "bar"}}}},
			result: &ValidationResult{
				Valid: true,
				StepGroups: []*StepGroupValidationResult{
					{
						ID:    "g01",
						Valid: true,
						Steps: []*StepValidationResult{
							{
								ID:         "g01-s01",
								Configured: true,
								Valid:      true,
								Inputs: []*InputValidationResult{
									{
										ID:      "foo",
										Visible: true,
									},
								},
							},
						},
					},
				},
			},
			values: input.Values{{ID: "foo", Value: "bar", Skipped: false}},
		},
		// Optional group, step is not configured
		{
			name: "optional-group-not-configured",
			groups: template.StepsGroups{
				{
					Required: input.RequiredOptional,
					Steps: template.Steps{
						{
							Inputs: template.Inputs{
								{ID: "foo", Type: input.TypeString, Kind: input.KindInput, Rules: "required"},
							},
						},
					},
				},
			},
			payload: []*StepPayload{},
			result: &ValidationResult{
				Valid: true,
				StepGroups: []*StepGroupValidationResult{
					{
						ID:    "g01",
						Valid: true, // <<<<<<<<<<<<<<<
						Steps: []*StepValidationResult{
							{
								ID:         "g01-s01",
								Configured: false, // <<<<<<<<<<<<<<<
								Valid:      true,
								Inputs: []*InputValidationResult{
									{
										ID:      "foo",
										Visible: true,
									},
								},
							},
						},
					},
				},
			},
			values: input.Values{{ID: "foo", Value: "", Skipped: true}},
		},
		// At least one rule, first step is configured, second is not configured
		{
			name: "optional-group-not-configured",
			groups: template.StepsGroups{
				{
					Required: input.RequiredAtLeastOne,
					Steps: template.Steps{
						{
							Inputs: template.Inputs{
								{ID: "foo", Type: input.TypeString, Kind: input.KindInput, Rules: "required"},
							},
						},
						{
							Inputs: template.Inputs{
								{ID: "baz", Type: input.TypeString, Kind: input.KindInput, Rules: "required"},
							},
						},
					},
				},
			},
			payload: []*StepPayload{{ID: "g01-s01", Inputs: []*InputValue{{ID: "foo", Value: "bar"}}}},
			result: &ValidationResult{
				Valid: true,
				StepGroups: []*StepGroupValidationResult{
					{
						ID:    "g01",
						Valid: true,
						Steps: []*StepValidationResult{
							{
								ID:         "g01-s01",
								Configured: true,
								Valid:      true,
								Inputs: []*InputValidationResult{
									{
										ID:      "foo",
										Visible: true,
									},
								},
							},
							{
								ID:         "g01-s02",
								Configured: false,
								Valid:      true,
								Inputs: []*InputValidationResult{
									{
										ID:      "baz",
										Visible: true,
									},
								},
							},
						},
					},
				},
			},
			values: input.Values{{ID: "foo", Value: "bar", Skipped: false}, {ID: "baz", Value: "", Skipped: true}},
		},
		// Step without inputs is always valid and configured
		{
			name: "empty-step",
			groups: template.StepsGroups{
				{
					Required: input.RequiredAtLeastOne,
					Steps: template.Steps{
						{
							Inputs: template.Inputs{},
						},
					},
				},
			},
			payload: []*StepPayload{},
			result: &ValidationResult{
				Valid: true,
				StepGroups: []*StepGroupValidationResult{
					{
						ID:    "g01",
						Valid: true, // <<<<<<<<<<<<<<<<<<<
						Steps: []*StepValidationResult{
							{
								ID:         "g01-s01",
								Configured: true, // <<<<<<<<<<<<<<<<<<<
								Valid:      true, // <<<<<<<<<<<<<<<<<<<
								Inputs:     []*InputValidationResult{},
							},
						},
					},
				},
			},
			values: input.Values{},
		},
		// Missing step payload
		{
			name: "missing-step",
			groups: template.StepsGroups{
				{
					Required: input.RequiredAtLeastOne,
					Steps: template.Steps{
						{
							Inputs: template.Inputs{
								{ID: "foo", Type: input.TypeString, Kind: input.KindInput, Rules: "required"},
							},
						},
					},
				},
			},
			payload: []*StepPayload{},
			result: &ValidationResult{
				Valid: false,
				StepGroups: []*StepGroupValidationResult{
					{
						ID:    "g01",
						Valid: false,
						Error: strPtr("At least one step must be selected."), // <<<<<<<<<<<<<<<<<<<
						Steps: []*StepValidationResult{
							{
								ID:         "g01-s01",
								Configured: false,
								Valid:      true,
								Inputs: []*InputValidationResult{
									{
										ID:      "foo",
										Visible: true,
									},
								},
							},
						},
					},
				},
			},
			values: input.Values{{ID: "foo", Value: "", Skipped: true}},
		},
		// Missing input value
		{
			name: "missing-input-value",
			groups: template.StepsGroups{
				{
					Required: input.RequiredAtLeastOne,
					Steps: template.Steps{
						{
							Inputs: template.Inputs{
								{ID: "foo", Name: "Foo Name", Type: input.TypeString, Kind: input.KindInput, Rules: "required"},
							},
						},
					},
				},
			},
			payload: []*StepPayload{{ID: "g01-s01", Inputs: []*InputValue{}}},
			result: &ValidationResult{
				Valid: false,
				StepGroups: []*StepGroupValidationResult{
					{
						ID:    "g01",
						Valid: false,
						Steps: []*StepValidationResult{
							{
								ID:         "g01-s01",
								Configured: true,
								Valid:      false,
								Inputs: []*InputValidationResult{
									{
										ID:      "foo",
										Visible: true,
										Error:   strPtr("Foo Name is a required field."), // <<<<<<<<<<<<<<<<<<<
									},
								},
							},
						},
					},
				},
			},
			values: input.Values{{ID: "foo", Value: "", Skipped: false}},
		},
	}

	// Test
	for i, c := range cases {
		desc := fmt.Sprintf("Case %d - %s", i+1, c.name)
		result, values, err := validateInputs(context.Background(), c.groups, c.payload)
		if c.err == "" {
			require.NoError(t, err)
			assert.Equal(t, c.result, result, desc)
			assert.Equal(t, c.values, values, desc)
		} else {
			require.Error(t, err)
			assert.Equal(t, c.err, err.Error(), desc)
		}
	}
}

func strPtr(str string) *string {
	return &str
}
