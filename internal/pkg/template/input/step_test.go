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
		Required:    "atLeastOne",
	}
	assert.NoError(t, g.ValidateStepsCount(2))
	err := g.ValidateStepsCount(0)
	assert.Error(t, err)
	assert.Equal(t, "at least one step must be selected", err.Error())

	g = StepsGroup{
		Description: "description",
		Required:    "zeroOrOne",
	}
	assert.NoError(t, g.ValidateStepsCount(0))
	assert.NoError(t, g.ValidateStepsCount(1))
	err = g.ValidateStepsCount(2)
	assert.Error(t, err)
	assert.Equal(t, "zero or one step must be selected", err.Error())

	g = StepsGroup{
		Description: "description",
		Required:    "exactlyOne",
	}
	assert.NoError(t, g.ValidateStepsCount(1))
	err = g.ValidateStepsCount(0)
	assert.Error(t, err)
	assert.Equal(t, "exactly one step must be selected", err.Error())
	err = g.ValidateStepsCount(2)
	assert.Error(t, err)
	assert.Equal(t, "exactly one step must be selected", err.Error())
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
							Id:          "fb.extractor.username",
							Name:        "Input",
							Description: "Description",
							Type:        "string",
							Kind:        "input",
						},
						{
							Id:          "fb.extractor.username",
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
							Id:          "fb.extractor.username",
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
							Id:          "fb.extractor.username",
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

	err := groups.Validate()
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())
}
