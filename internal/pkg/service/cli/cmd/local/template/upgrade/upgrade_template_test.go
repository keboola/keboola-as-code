package upgrade

import (
	"sync"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	upgradeTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
)

const (
	DownArrow = "\u001B[B"
	Space     = " "
	Enter     = "\n"
)

func TestAskUpgradeTemplate(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	projectState := deps.MockedState()

	// Project state
	instance := model.TemplateInstance{InstanceID: "12345abc"}
	branchKey := model.BranchKey{ID: 123}
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "111"}
	configRowKey := model.ConfigRowKey{BranchID: 123, ComponentID: "foo.bar", ConfigID: "111", ID: "222"}
	configMetadata := model.ConfigMetadata{}
	configContent := orderedmap.New()
	rowContent := orderedmap.New()
	configMetadata.AddInputUsage("input1", orderedmap.PathFromStr("foo.bar"), nil)
	require.NoError(t, configContent.SetNested("foo.bar", "old value 1")) // <<<<<<<<<<<
	configMetadata.AddRowInputUsage(configRowKey.ID, "input2", orderedmap.PathFromStr("foo.bar"), nil)
	require.NoError(t, rowContent.SetNested("foo.bar", "old value 2")) // <<<<<<<<<<<
	require.NoError(t, projectState.Set(&model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local:          &model.Config{ConfigKey: configKey, Metadata: configMetadata, Content: configContent},
	}))
	require.NoError(t, projectState.Set(&model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: configRowKey},
		Local:             &model.ConfigRow{ConfigRowKey: configRowKey, Content: rowContent},
	}))

	// Set instance ID
	configMetadata.SetTemplateInstance("repo", "template", instance.InstanceID)
	configMetadata.SetConfigTemplateID("configInTemplate")
	configMetadata.AddRowTemplateID(configRowKey.ID, "rowInTemplate")

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
							ID:   "input1",
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
							ID:      "input2",
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
							ID:   "input3",
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
							ID:   "input4",
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
	d.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, console.ExpectString("Please select which steps you want to fill."))

		// Step 1 and 2 are pre-selected, because Input 1 and 2 have been found in config/row.
		require.NoError(t, console.ExpectString("Select steps:"))

		require.NoError(t, console.ExpectString("[x]"))

		require.NoError(t, console.ExpectString("Step 1 - Step Description"))

		require.NoError(t, console.ExpectString("[x]"))

		require.NoError(t, console.ExpectString("Step 2 - Step Description"))

		require.NoError(t, console.ExpectString("[ ]"))

		require.NoError(t, console.ExpectString("Step 3 - Step Description"))

		require.NoError(t, console.ExpectString("[ ]"))

		require.NoError(t, console.ExpectString("Step 4 - Step Description"))

		require.NoError(t, console.Send(DownArrow)) // move to step 4

		require.NoError(t, console.Send(DownArrow))

		require.NoError(t, console.Send(DownArrow))

		require.NoError(t, console.Send(Space)) // select step 4

		require.NoError(t, console.Send(Enter)) // confirm the selection

		require.NoError(t, console.ExpectString("Step 1"))

		require.NoError(t, console.ExpectString("input1:"))

		require.NoError(t, console.ExpectString("(old value 1)"))

		require.NoError(t, console.SendLine(Enter)) // use default/old value

		require.NoError(t, console.ExpectString("Step 2"))

		require.NoError(t, console.ExpectString("input2:"))

		require.NoError(t, console.ExpectString("(old value 2)"))

		require.NoError(t, console.SendLine("new value 2")) // fill new value

		require.NoError(t, console.ExpectString("Step 4"))

		require.NoError(t, console.ExpectString("input4:"))

		require.NoError(t, console.SendLine("value 4"))

		require.NoError(t, console.ExpectEOF())
	}()

	output, err := AskUpgradeTemplateOptions(t.Context(), d, deps, projectState, branchKey, instance, stepsGroups, configmap.NewValue("input4"))
	require.NoError(t, err)

	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, upgradeTemplate.Options{
		Instance: instance,
		Branch:   branchKey,
		Inputs: template.InputsValues{
			{ID: "input1", Value: "old value 1"},
			{ID: "input2", Value: "new value 2"},
			{ID: "input3", Value: "", Skipped: true},
			{ID: "input4", Value: "value 4"},
		},
	}, output)
}
