package dialog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
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
icon: common:settings

## Group
description: Group Description
required: all

### Step "s2"
name: Step Two
description: Description
icon: common:settings

### Step "s3"
name: Step Three
description: Description
icon: common:settings

`

	expectedGroups := input.StepsGroupsExt{
		&input.StepsGroupExt{
			GroupIndex: 0,
			StepsGroup: input.StepsGroup{Description: "Group Description", Required: "all"},
			Steps: input.StepsExt{
				{GroupIndex: 0, StepIndex: 0, ID: "s1", Step: input.Step{Icon: "common:settings", Name: "Step One", Description: "Description"}},
			},
		},
		&input.StepsGroupExt{
			GroupIndex: 1,
			StepsGroup: input.StepsGroup{Description: "Group Description", Required: "all"},
			Steps: input.StepsExt{
				{GroupIndex: 1, StepIndex: 0, ID: "s2", Step: input.Step{Icon: "common:settings", Name: "Step Two", Description: "Description"}},
				{GroupIndex: 1, StepIndex: 1, ID: "s3", Step: input.Step{Icon: "common:settings", Name: "Step Three", Description: "Description"}},
			},
		},
	}

	// Parse
	d := newStepsDialog(nopPrompt.New())
	stepsGroups, err := d.parse(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, expectedGroups, stepsGroups)
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
icon: common:settings

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
- group 1: "required" must be one of [all atLeastOne exactlyOne zeroOrOne optional]
- group 1, step 1: "icon" is a required field
- group 1, step 1: "name" must be a maximum of 25 characters in length
- group 2: "required" must be one of [all atLeastOne exactlyOne zeroOrOne optional]
- group 2: "steps" must contain at least 1 step
- group 3: "required" must be one of [all atLeastOne exactlyOne zeroOrOne optional]
- group 3, step 1: "icon" is a required field
- group 3, step 2: "icon" is a required field
`

	// Parse
	d := newStepsDialog(nopPrompt.New())
	_, err := d.parse(t.Context(), in)
	require.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}

func TestStepsDialog_Parse_NoGroups(t *testing.T) {
	t.Parallel()

	in := `
`

	expected := `
at least one steps group must be defined
`

	// Parse
	d := newStepsDialog(nopPrompt.New())
	_, err := d.parse(t.Context(), in)
	require.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}
