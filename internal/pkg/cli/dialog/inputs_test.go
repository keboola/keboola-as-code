package dialog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestInputsAddInputsToStepsGroups(t *testing.T) {
	t.Parallel()

	stepsGroups := input.StepsGroups{
		&input.StepsGroup{Description: "desc", Required: "all", Steps: []*input.Step{
			{Icon: "common", Name: "Step One", Description: "Description"},
		}},
		&input.StepsGroup{Required: "all", Steps: []*input.Step{
			{Icon: "common", Name: "Step Two", Description: "Description"},
			{Icon: "common", Name: "Step Three", Description: "Description"},
		}},
	}
	stepsToIds := map[input.StepIndex]string{
		input.StepIndex{Step: 0, Group: 0}: "s1",
		input.StepIndex{Step: 0, Group: 1}: "s2",
		input.StepIndex{Step: 1, Group: 1}: "s3",
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
	i2sMap := orderedmap.New()
	i2sMap.Set("i1", "s2")
	i2sMap.Set("i2", "s4")
	err := addInputsToStepsGroups(stepsGroups, inputs, i2sMap, stepsToIds)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "input \"i2\": step \"s4\" not found")
	i, f := stepsGroups.InputsForStep(input.StepIndex{Step: 0, Group: 1})
	assert.True(t, f)
	assert.Equal(t, input.Inputs{*input1}, i)
}
