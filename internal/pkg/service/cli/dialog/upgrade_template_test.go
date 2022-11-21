package dialog_test

import (
	"sync"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	upgradeTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
)

func TestAskUpgradeTemplate(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	projectState := d.MockedState()

	// Project state
	instance := model.TemplateInstance{InstanceId: "12345abc"}
	branchKey := model.BranchKey{Id: 123}
	configKey := model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", Id: "111"}
	configRowKey := model.ConfigRowKey{BranchId: 123, ComponentId: "foo.bar", ConfigId: "111", Id: "222"}
	configMetadata := model.ConfigMetadata{}
	configContent := orderedmap.New()
	rowContent := orderedmap.New()
	configMetadata.AddInputUsage("input1", orderedmap.PathFromStr("foo.bar"), nil)
	assert.NoError(t, configContent.SetNested("foo.bar", "old value 1")) // <<<<<<<<<<<
	configMetadata.AddRowInputUsage(configRowKey.Id, "input2", orderedmap.PathFromStr("foo.bar"), nil)
	assert.NoError(t, rowContent.SetNested("foo.bar", "old value 2")) // <<<<<<<<<<<
	assert.NoError(t, projectState.Set(&model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local:          &model.Config{ConfigKey: configKey, Metadata: configMetadata, Content: configContent},
	}))
	assert.NoError(t, projectState.Set(&model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: configRowKey},
		Local:             &model.ConfigRow{ConfigRowKey: configRowKey, Content: rowContent},
	}))

	// Set instance ID
	configMetadata.SetTemplateInstance("repo", "template", instance.InstanceId)
	configMetadata.SetConfigTemplateId("configInTemplate")
	configMetadata.AddRowTemplateId(configRowKey.Id, "rowInTemplate")

	// Template inputs
	stepsGroups := input.StepsGroups{
		{
			Description: "Please select which steps you want to fill.",
			Required:    "optional",
			Steps: []input.Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							Id:   "input1",
							Name: "input1",
							Type: "string",
							Kind: "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 2",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							Id:      "input2",
							Name:    "input2",
							Type:    "string",
							Kind:    "input",
							Default: "default value",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 3",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							Id:   "input3",
							Name: "input3",
							Type: "string",
							Kind: "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 4",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							Id:   "input4",
							Name: "input4",
							Type: "string",
							Kind: "input",
						},
					},
				},
			},
		},
	}

	// Set fake file editor
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Please select which steps you want to fill."))

		// Step 1 and 2 are pre-selected, because Input 1 and 2 have been found in config/row.
		assert.NoError(t, console.ExpectString("Select steps:"))

		assert.NoError(t, console.ExpectString("[x]"))

		assert.NoError(t, console.ExpectString("Step 1 - Step Description"))

		assert.NoError(t, console.ExpectString("[x]"))

		assert.NoError(t, console.ExpectString("Step 2 - Step Description"))

		assert.NoError(t, console.ExpectString("[ ]"))

		assert.NoError(t, console.ExpectString("Step 3 - Step Description"))

		assert.NoError(t, console.ExpectString("[ ]"))

		assert.NoError(t, console.ExpectString("Step 4 - Step Description"))

		assert.NoError(t, console.Send(DownArrow)) // move to step 4

		assert.NoError(t, console.Send(DownArrow))

		assert.NoError(t, console.Send(DownArrow))

		assert.NoError(t, console.Send(Space)) // select step 4

		assert.NoError(t, console.Send(Enter)) // confirm the selection

		assert.NoError(t, console.ExpectString("Step 1"))

		assert.NoError(t, console.ExpectString("input1:"))

		assert.NoError(t, console.ExpectString("(old value 1)"))

		assert.NoError(t, console.SendLine(Enter)) // use default/old value

		assert.NoError(t, console.ExpectString("Step 2"))

		assert.NoError(t, console.ExpectString("input2:"))

		assert.NoError(t, console.ExpectString("(old value 2)"))

		assert.NoError(t, console.SendLine("new value 2")) // fill new value

		assert.NoError(t, console.ExpectString("Step 4"))

		assert.NoError(t, console.ExpectString("input4:"))

		assert.NoError(t, console.SendLine("value 4"))

		assert.NoError(t, console.ExpectEOF())
	}()

	output, err := dialog.AskUpgradeTemplateOptions(d, projectState, branchKey, instance, stepsGroups)
	assert.NoError(t, err)

	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, upgradeTemplate.Options{
		Instance: instance,
		Branch:   branchKey,
		Inputs: template.InputsValues{
			{Id: "input1", Value: "old value 1"},
			{Id: "input2", Value: "new value 2"},
			{Id: "input3", Value: "", Skipped: true},
			{Id: "input4", Value: "value 4"},
		},
	}, output)
}
