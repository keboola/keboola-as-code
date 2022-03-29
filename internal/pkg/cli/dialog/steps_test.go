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
description: desc
required: all

### Step "s1"
name: Step One
description: Description
icon: common

## Group
description:
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

	expected := input.StepsGroups{
		&input.StepsGroup{Description: "desc", Required: "all", Steps: []*input.Step{
			{Id: "s1", Icon: "common", Name: "Step One", Description: "Description"},
		}},
		&input.StepsGroup{Required: "all", Steps: []*input.Step{
			{Id: "s2", Icon: "common", Name: "Step Two", Description: "Description"},
			{Id: "s3", Icon: "common", Name: "Step Three", Description: "Description"},
		}},
	}

	// Parse
	d := newStepsDialog(nopPrompt.New())
	err := d.parse(in)
	assert.NoError(t, err)
	assert.Equal(t, expected, d.stepsGroups)
}

func TestStepsDialog_Parse_Errors(t *testing.T) {
	t.Parallel()

	// Validace neřeší json tagy (oneOf u required a max length u descriptions)
	in := `
### Step "s0"
name: Step 0
description: Description

## Group
description:
required: invalid

### Step "s1"
name: toooooooooooooooooooooooooooooooo long name
required: all
description: Description

## Group
description:

## Group
description:

### Step "s2"
name: Step Two
description: Description

### Step "s3"
name: Step Three
description: Description

`

	expected := `
- line 2: there is no group for step "s0"
- line 12: required is not valid option for a step
- group 1: required must be one of [all atLeastOne exactOne zeroOrOne optional]
- group 1, step "s1": icon is a required field
- group 1, step "s1": name must be a maximum of 20 characters in length
- group 2: required must be one of [all atLeastOne exactOne zeroOrOne optional]
- group 2: steps must contain at least 1 item
- group 3: required must be one of [all atLeastOne exactOne zeroOrOne optional]
- group 3, step "s2": icon is a required field
- group 3, step "s3": icon is a required field
`

	// Parse
	d := newStepsDialog(nopPrompt.New())
	err := d.parse(in)
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}
