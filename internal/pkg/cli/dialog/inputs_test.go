package dialog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

func TestInputsAddInputsToStepsGroups(t *testing.T) {
	t.Parallel()

	stepsGroups := &input.StepsGroups{
		&input.StepsGroup{Description: "desc", Required: "all", Steps: []*input.Step{
			{Id: "1", Icon: "common", Name: "Step One", Description: "Description"},
		}},
		&input.StepsGroup{Required: "all", Steps: []*input.Step{
			{Id: "2", Icon: "common", Name: "Step Two", Description: "Description"},
			{Id: "3", Icon: "common", Name: "Step Three", Description: "Description"},
		}},
	}
	inputs := newInputsMap()
	input1 := &template.Input{
		Id: "i1",
	}
	inputs.add(input1)
	input2 := &template.Input{
		Id: "i2",
	}
	inputs.add(input2)
	i2sMap := make(map[string]string)
	i2sMap["i1"] = "2"
	i2sMap["i2"] = "4"
	err := addInputsToStepsGroups(stepsGroups, inputs, i2sMap)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "input \"i2\": step \"4\" not found")
	i, f := stepsGroups.InputsForStep(input.StepIndex{Step: 0, Group: 1})
	assert.True(t, f)
	assert.Equal(t, input.Inputs{*input1}, i)
}
