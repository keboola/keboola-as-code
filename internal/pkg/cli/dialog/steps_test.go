package dialog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

func TestStepsDialog_Parse_Ok(t *testing.T) {
	t.Parallel()

	in := `
## Group
description: Group Description
required: all

### Step "s1"
name: Step One
description: Description
icon: common

## Group
description: Group Description
required: all

### Step "s2"
name: Step Two
description: Description
icon: common

### Step "s3"
name: Step Three
description: Description
icon: common

`

	expectedGroups := input.StepsGroups{
		&input.StepsGroup{Description: "Group Description", Required: "all", Steps: []*input.Step{
			{Icon: "common", Name: "Step One", Description: "Description", Inputs: make(input.Inputs, 0)},
		}},
		&input.StepsGroup{Description: "Group Description", Required: "all", Steps: []*input.Step{
			{Icon: "common", Name: "Step Two", Description: "Description", Inputs: make(input.Inputs, 0)},
			{Icon: "common", Name: "Step Three", Description: "Description", Inputs: make(input.Inputs, 0)},
		}},
	}

	expectedMap := map[input.StepIndex]string{
		{Step: 0, Group: 0}: "s1",
		{Step: 0, Group: 1}: "s2",
		{Step: 1, Group: 1}: "s3",
	}

	// Parse
	d := newStepsDialog(nopPrompt.New())
	stepsGroups, stepsToIds, err := d.parse(in)
	assert.NoError(t, err)
	assert.Equal(t, expectedGroups, stepsGroups)
	assert.Equal(t, expectedMap, stepsToIds)
}

func TestStepsDialog_Parse_Errors(t *testing.T) {
	t.Parallel()

	in := `
### Step "s0"
name: Step 0
description: Description

## Group
description: Group description.
required: invalid

### Step "s1"
name: toooooooooooooooooooooooooooooooo long name
required: all
description: Description

## Group
description: Group Description

## Group
description: Group Description

### Step "s1"
name: Step One again
description: Description
icon: common

### Step "s2"
name: Step Two
description: Description

### Step "s3"
name: Step Three
description: Description

`

	expected := `
- line 2: there needs to be a group definition before step "s0"
- line 12: required is not valid option for a step
- line 21: step with id "s1" is already defined
- group 1: required must be one of [all atLeastOne exactlyOne zeroOrOne optional]
- group 1, step 1: icon is a required field
- group 1, step 1: name must be a maximum of 25 characters in length
- group 2: required must be one of [all atLeastOne exactlyOne zeroOrOne optional]
- group 2: steps must contain at least 1 step
- group 3: required must be one of [all atLeastOne exactlyOne zeroOrOne optional]
- group 3, step 2: icon is a required field
- group 3, step 3: icon is a required field
`

	// Parse
	d := newStepsDialog(nopPrompt.New())
	_, _, err := d.parse(in)
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}

func TestStepsDialog_Parse_NoGroups(t *testing.T) {
	t.Parallel()

	in := `
`

	expected := `
at least 1 group must be defined
`

	// Parse
	d := newStepsDialog(nopPrompt.New())
	_, _, err := d.parse(in)
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}
