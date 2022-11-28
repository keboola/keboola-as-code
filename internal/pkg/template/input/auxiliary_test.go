package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStepsGroups_ToExtended(t *testing.T) {
	t.Parallel()

	// Fixtures
	input1 := Input{
		ID:          "foo",
		Name:        "foo",
		Description: "description",
		Type:        "string",
		Kind:        "input",
	}
	input2 := Input{
		ID:          "bar",
		Name:        "bar",
		Description: "description",
		Type:        "string",
		Kind:        "input",
	}
	input3 := Input{
		ID:          "baz",
		Name:        "baz",
		Description: "description",
		Type:        "string",
		Kind:        "input",
	}
	step1 := Step{
		Icon:        "common:settings",
		Name:        "Step 1",
		Description: "Step One",
		Inputs:      Inputs{input1},
	}
	step2 := Step{
		Icon:        "common:settings",
		Name:        "Step 2",
		Description: "Step Two",
		Inputs:      Inputs{input2},
	}
	step3 := Step{
		Icon:        "common:settings",
		Name:        "Step 3",
		Description: "Step Three",
		Inputs:      Inputs{input3},
	}
	stepGroup1 := StepsGroup{
		Description: "Group One",
		Required:    "all",
		Steps:       []Step{step1, step2},
	}
	stepGroup2 := StepsGroup{
		Description: "Group Two",
		Required:    "all",
		Steps:       []Step{step3},
	}
	stepsGroups := StepsGroups{stepGroup1, stepGroup2}

	// Expected result
	expected := StepsGroupsExt{
		{
			ID:         "g01",
			GroupIndex: 0,
			StepsGroup: stepGroup1,
			Steps: []*StepExt{
				{
					ID:         "g01-s01",
					GroupIndex: 0,
					StepIndex:  0,
					Step:       step1,
				},
				{
					ID:         "g01-s02",
					GroupIndex: 0,
					StepIndex:  1,
					Step:       step2,
				},
			},
		},
		{
			ID:         "g02",
			GroupIndex: 1,
			StepsGroup: stepGroup2,
			Steps: []*StepExt{
				{
					ID:         "g02-s01",
					GroupIndex: 1,
					StepIndex:  0,
					Step:       step3,
				},
			},
		},
	}

	assert.Equal(t, expected, stepsGroups.ToExtended())
}
