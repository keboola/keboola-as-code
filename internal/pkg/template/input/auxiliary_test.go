package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStepsGroups_ToExtended(t *testing.T) {
	t.Parallel()

	// Fixtures
	step1 := &Step{
		Icon:        "common:settings",
		Name:        "Step 1",
		Description: "Step One",
		Inputs: Inputs{
			{
				Id:          "foo",
				Name:        "foo",
				Description: "description",
				Type:        "string",
				Kind:        "input",
			},
		},
	}
	step2 := &Step{
		Icon:        "common:settings",
		Name:        "Step 2",
		Description: "Step Two",
		Inputs: Inputs{
			{
				Id:          "foo",
				Name:        "foo",
				Description: "description",
				Type:        "string",
				Kind:        "input",
			},
		},
	}
	step3 := &Step{
		Icon:        "common:settings",
		Name:        "Step 3",
		Description: "Step Three",
		Inputs: Inputs{
			{
				Id:          "baz",
				Name:        "baz",
				Description: "description",
				Type:        "string",
				Kind:        "input",
			},
		},
	}
	stepGroup1 := &StepsGroup{
		Description: "Group One",
		Required:    "all",
		Steps:       []*Step{step1, step2},
	}
	stepGroup2 := &StepsGroup{
		Description: "Group Two",
		Required:    "all",
		Steps:       []*Step{step3},
	}
	stepsGroups := StepsGroups{stepGroup1, stepGroup2}

	// Expected result
	expected := StepsGroupsExt{
		{
			Id:         "g01",
			GroupIndex: 0,
			StepsGroup: *stepGroup1,
			Steps: []*StepExt{
				{
					Id:         "g01-s01",
					GroupIndex: 0,
					StepIndex:  0,
					Step:       *step1,
				},
				{
					Id:         "g01-s02",
					GroupIndex: 0,
					StepIndex:  1,
					Step:       *step2,
				},
			},
		},
		{
			Id:         "g02",
			GroupIndex: 1,
			StepsGroup: *stepGroup2,
			Steps: []*StepExt{
				{
					Id:         "g02-s01",
					GroupIndex: 1,
					StepIndex:  0,
					Step:       *step3,
				},
			},
		},
	}

	assert.Equal(t, expected, stepsGroups.ToExtended())
}
