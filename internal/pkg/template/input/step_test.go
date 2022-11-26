package input

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStepsGroup_AreStepsSelectable(t *testing.T) {
	t.Parallel()

	// Do not show select for required == "all"
	g := StepsGroup{
		Description: "description",
		Required:    "all",
		Steps: Steps{
			{Name: "Step 1"},
			{Name: "Step 2"},
		},
	}
	assert.False(t, g.AreStepsSelectable())

	// Do not show select for required == "exactlyOne" and one step
	g = StepsGroup{
		Description: "description",
		Required:    "exactlyOne",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.False(t, g.AreStepsSelectable())

	// Do not show select for required == "atLeastOne" and one step
	g = StepsGroup{
		Description: "description",
		Required:    "atLeastOne",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.False(t, g.AreStepsSelectable())

	// Show select for required == "optional"
	g = StepsGroup{
		Description: "description",
		Required:    "optional",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.True(t, g.AreStepsSelectable())

	// Show select for required == "zeroOrOne"
	g = StepsGroup{
		Description: "description",
		Required:    "zeroOrOne",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.True(t, g.AreStepsSelectable())
}

func TestStepsGroup_ValidateSelectedSteps(t *testing.T) {
	t.Parallel()

	g := StepsGroup{
		Description: "description",
		Required:    RequiredAtLeastOne,
	}
	assert.NoError(t, g.ValidateStepsCount(10, 2))
	err := g.ValidateStepsCount(10, 0)
	assert.Error(t, err)
	assert.Equal(t, "at least one step must be selected", err.Error())

	g = StepsGroup{
		Description: "description",
		Required:    RequiredZeroOrOne,
	}
	assert.NoError(t, g.ValidateStepsCount(10, 0))
	assert.NoError(t, g.ValidateStepsCount(10, 1))
	err = g.ValidateStepsCount(10, 2)
	assert.Error(t, err)
	assert.Equal(t, "zero or one step must be selected", err.Error())

	g = StepsGroup{
		Description: "description",
		Required:    RequiredExactlyOne,
	}
	assert.NoError(t, g.ValidateStepsCount(10, 1))
	err = g.ValidateStepsCount(10, 0)
	assert.Error(t, err)
	assert.Equal(t, "exactly one step must be selected", err.Error())
	err = g.ValidateStepsCount(10, 2)
	assert.Error(t, err)
	assert.Equal(t, "exactly one step must be selected", err.Error())

	g = StepsGroup{
		Description: "description",
		Required:    RequiredAll,
	}
	assert.NoError(t, g.ValidateStepsCount(10, 10))
	err = g.ValidateStepsCount(10, 9)
	assert.Error(t, err)
	assert.Equal(t, "all steps (10) must be selected", err.Error())
}

func TestStepsGroups_Validate_DuplicateInputs(t *testing.T) {
	t.Parallel()

	groups := StepsGroups{
		{
			Description: "Group One",
			Required:    "all",
			Steps: []Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step One",
					Inputs: Inputs{
						{
							ID:          "fb.extractor.username",
							Name:        "Input",
							Description: "Description",
							Type:        "string",
							Kind:        "input",
						},
						{
							ID:          "fb.extractor.username",
							Name:        "Input",
							Description: "Description",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
			},
		},
		{
			Description: "Group Two",
			Required:    "all",
			Steps: []Step{
				{
					Icon:        "common:settings",
					Name:        "Step 2",
					Description: "Step Two",
					Inputs: Inputs{
						{
							ID:          "fb.extractor.username",
							Name:        "Input",
							Description: "Description",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 3",
					Description: "Step Three",
					Inputs: Inputs{
						{
							ID:          "fb.extractor.username",
							Name:        "Input",
							Description: "Description",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
			},
		},
	}

	// Assert
	expectedErr := `
input "fb.extractor.username" is defined 4 times in:
  - group 1, step 1 "Step 1"
  - group 1, step 1 "Step 1"
  - group 2, step 1 "Step 2"
  - group 2, step 2 "Step 3"
`

	err := groups.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())
}

func TestStepsGroups_Validate_InputsErrors(t *testing.T) {
	t.Parallel()

	groups := StepsGroups{
		{
			Description: "Group One",
			Required:    "all",
			Steps: []Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step One",
					Inputs: Inputs{
						{
							ID:          "input1",
							Name:        "Input",
							Description: "Description",
							Type:        "foo",
							Kind:        "input",
						},
						{
							ID:          "input2",
							Name:        "Input",
							Description: "Description",
							Type:        "bar",
							Kind:        "input",
						},
					},
				},
			},
		},
	}

	// Assert
	expectedErr := `
- group 1, step 1, input "input1": "type" foo is not allowed, allowed values: string, int, double, bool, string[], object
- group 1, step 1, input "input2": "type" bar is not allowed, allowed values: string, int, double, bool, string[], object
`

	err := groups.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())
}
