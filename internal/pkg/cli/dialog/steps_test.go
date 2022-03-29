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
## Group "Group 1"
description: desc
required: all

### Step "Step 1"
name: Step One
description: Description
icon: common

## Group "Group 2"
description:
required: all

### Step "Step 2"
name: Step Two
description: Description
icon: common

### Step "Step 3"
name: Step Three
description: Description
icon: common

`

	expected := input.StepsGroups{
		&input.StepsGroup{Id: "Group 1", Description: "desc", Required: "all", Steps: []*input.Step{
			{Id: "Step 1", Icon: "common", Name: "Step One", Description: "Description"},
		}},
		&input.StepsGroup{Id: "Group 2", Required: "all", Steps: []*input.Step{
			{Id: "Step 2", Icon: "common", Name: "Step Two", Description: "Description"},
			{Id: "Step 3", Icon: "common", Name: "Step Three", Description: "Description"},
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
### Step "Step 0"
name: Step 0
description: Description

## Group "Group 1"
description:
required: invalid

### Step "Step 1"
name: toooooooooooooooooooooooooooooooo long name
required: all
description: Description

## Group "Empty Group"
description:

## Group "Group 2"
description:

### Step "Step 2"
name: Step Two
description: Description

### Step "Step 3"
name: Step Three
description: Description

`

	expected := `
- line 2: there is no group for step "Step 0"
- line 12: required is not valid option for a step
- group "Group 1": required must be one of [all atLeastOne exactOne zeroOrOne optional]
- group "Group 1", step "Step 1": icon is a required field
- group "Group 1", step "Step 1": name must be a maximum of 20 characters in length
- group "Empty Group": required must be one of [all atLeastOne exactOne zeroOrOne optional]
- group "Empty Group": steps must contain at least 1 item
- group "Group 2": required must be one of [all atLeastOne exactOne zeroOrOne optional]
- group "Group 2", step "Step 2": icon is a required field
- group "Group 2", step "Step 3": icon is a required field
`

	// Parse
	d := newStepsDialog(nopPrompt.New())
	err := d.parse(in)
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}
