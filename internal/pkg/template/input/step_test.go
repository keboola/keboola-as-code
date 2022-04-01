package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStepsGroup_ShowStepsSelect(t *testing.T) {
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
	assert.False(t, g.ShowStepsSelect())

	// Do not show select for required == "exactlyOne" and one step
	g = StepsGroup{
		Description: "description",
		Required:    "exactlyOne",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.False(t, g.ShowStepsSelect())

	// Do not show select for required == "atLeastOne" and one step
	g = StepsGroup{
		Description: "description",
		Required:    "atLeastOne",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.False(t, g.ShowStepsSelect())

	// Show select for required == "optional"
	g = StepsGroup{
		Description: "description",
		Required:    "optional",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.True(t, g.ShowStepsSelect())

	// Show select for required == "zeroOrOne"
	g = StepsGroup{
		Description: "description",
		Required:    "zeroOrOne",
		Steps: Steps{
			{Name: "Step 1"},
		},
	}
	assert.True(t, g.ShowStepsSelect())
}

func TestStepsGroup_ValidateSelectedSteps(t *testing.T) {
	t.Parallel()

	g := StepsGroup{
		Description: "description",
		Required:    "atLeastOne",
	}
	assert.NoError(t, g.ValidateSelectedSteps(2))
	err := g.ValidateSelectedSteps(0)
	assert.Error(t, err)
	assert.Equal(t, "at least one step must be selected", err.Error())

	g = StepsGroup{
		Description: "description",
		Required:    "zeroOrOne",
	}
	assert.NoError(t, g.ValidateSelectedSteps(0))
	assert.NoError(t, g.ValidateSelectedSteps(1))
	err = g.ValidateSelectedSteps(2)
	assert.Error(t, err)
	assert.Equal(t, "zero or one step must be selected", err.Error())

	g = StepsGroup{
		Description: "description",
		Required:    "exactlyOne",
	}
	assert.NoError(t, g.ValidateSelectedSteps(1))
	err = g.ValidateSelectedSteps(0)
	assert.Error(t, err)
	assert.Equal(t, "exactly one step must be selected", err.Error())
	err = g.ValidateSelectedSteps(2)
	assert.Error(t, err)
	assert.Equal(t, "exactly one step must be selected", err.Error())
}
