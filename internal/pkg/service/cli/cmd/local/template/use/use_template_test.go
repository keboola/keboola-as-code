package use

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

const Backspace = "\b"

// If condition for restricted input is met by setting the age above the limit.
func TestAskUseTemplate_ShowIfMet(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(t.Context(), loadState.Options{LoadLocalState: true}, deps)
	require.NoError(t, err)

	// Set fake file editor
	d.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {

		require.NoError(t, console.ExpectString("Select the target branch:"))

		require.NoError(t, console.SendEnter()) // enter - Main

		require.NoError(t, console.ExpectString("Instance Name:"))

		require.NoError(t, console.SendLine("My Instance"))

		require.NoError(t, console.ExpectString("Enter your Facebook username"))

		require.NoError(t, console.ExpectString("Facebook username"))

		// username can contain alphanum only
		require.NoError(t, console.SendLine("u-s"))

		require.NoError(t, console.ExpectString(`Facebook username can only contain alphanumeric characters`))

		require.NoError(t, console.Send(strings.Repeat(Backspace, 3))) // remove "u-s"

		require.NoError(t, console.SendLine("username"))

		require.NoError(t, console.ExpectString("Enter your Facebook password"))

		require.NoError(t, console.ExpectString("Facebook password"))

		require.NoError(t, console.SendLine("password"))

		require.NoError(t, console.ExpectString("Enter your age"))

		require.NoError(t, console.SendLine("text")) // enter invalid string value

		require.NoError(t, console.ExpectString(`Sorry, your reply was invalid: value "text" is not integer`))

		require.NoError(t, console.Send(strings.Repeat(Backspace, 4))) // remove "text"

		require.NoError(t, console.SendLine("25")) // enter valid numeric value

		require.NoError(t, console.ExpectString("Do you want to see restricted content?"))

		require.NoError(t, console.ExpectString("Restricted content"))

		require.NoError(t, console.SendLine("yes"))

		require.NoError(t, console.ExpectString("What do you like to drink?"))

		require.NoError(t, console.ExpectString("Favorite drink"))

		require.NoError(t, console.ExpectString("Beer"))

		require.NoError(t, console.ExpectString("Wine"))

		require.NoError(t, console.SendDownArrow()) // -> Wine

		require.NoError(t, console.SendSpace()) // -> select

		require.NoError(t, console.SendEnter()) // -> confirm

		require.NoError(t, console.ExpectString("Anything stronger?"))

		require.NoError(t, console.ExpectString("Stronger drinks"))

		require.NoError(t, console.ExpectString("Rum"))

		require.NoError(t, console.ExpectString("Vodka"))

		require.NoError(t, console.ExpectString("Whiskey"))

		require.NoError(t, console.SendSpace()) // -> select

		require.NoError(t, console.SendDownArrow()) // -> Vodka

		require.NoError(t, console.SendDownArrow()) // -> Whiskey

		require.NoError(t, console.SendSpace()) // -> select

		require.NoError(t, console.SendEnter()) // -> confirm

		require.NoError(t, console.ExpectEOF())
	})

	// Run
	inputs := []input.Input{
		{
			ID:          "facebook.username",
			Name:        "Facebook username",
			Description: "Enter your Facebook username",
			Type:        "string",
			Kind:        "input",
			Rules:       "alphanum",
		},
		{
			ID:          "facebook.password",
			Name:        "Facebook password",
			Description: "Enter your Facebook password",
			Type:        "string",
			Kind:        "hidden",
		},
		{
			ID:          "age",
			Name:        "Your age",
			Description: "Enter your age",
			Type:        "int",
			Kind:        "input",
		},
		{
			ID:          "restricted",
			Name:        "Restricted content",
			Description: "Do you want to see restricted content?",
			Type:        "bool",
			Kind:        "confirm",
			If:          "age>18",
		},
		{
			ID:          "drink",
			Name:        "Favorite drink",
			Description: "What do you like to drink?",
			Type:        "string",
			Kind:        "select",
			If:          "age>18",
			Options:     input.Options{{Value: "beer", Label: "Beer"}, {Value: "wine", Label: "Wine"}},
		},
		{
			ID:          "drinks",
			Name:        "Stronger drinks",
			Description: "Anything stronger?",
			Type:        "string[]",
			Kind:        "multiselect",
			If:          "age>18",
			Options:     input.Options{{Value: "rum", Label: "Rum"}, {Value: "vodka", Label: "Vodka"}, {Value: "whiskey", Label: "Whiskey"}},
		},
	}

	stepsGroups := input.StepsGroups{
		{
			Description: "Group One",
			Required:    "all",
			Steps: []input.Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step One",
					Inputs:      inputs,
				},
			},
		},
	}

	f := Flags{
		Branch:       configmap.NewValue("123"),
		InstanceName: configmap.NewValue("My Instance"),
	}

	output, err := AskUseTemplateOptions(t.Context(), d, projectState, stepsGroups, f)
	require.NoError(t, err)

	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{ID: 123},
		Inputs: template.InputsValues{
			{ID: "facebook.username", Value: "username"},
			{ID: "facebook.password", Value: "password"},
			{ID: "age", Value: 25},
			{ID: "restricted", Value: true},
			{ID: "drink", Value: "wine"},
			{ID: "drinks", Value: []any{"rum", "whiskey"}},
		},
	}, output)
}

// If condition for restricted input is not met by setting the age below the limit and so that input is not shown to the user.
func TestAskUseTemplate_ShowIfNotMet(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(t.Context(), loadState.Options{LoadLocalState: true}, deps)
	require.NoError(t, err)

	// Set fake file editor
	d.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {

		require.NoError(t, console.ExpectString("Select the target branch:"))

		require.NoError(t, console.SendEnter()) // enter - Main

		require.NoError(t, console.ExpectString("Instance Name:"))

		require.NoError(t, console.SendLine("My Instance"))

		require.NoError(t, console.ExpectString("Enter your Facebook username"))

		require.NoError(t, console.SendLine("username"))

		require.NoError(t, console.ExpectString("Enter your Facebook password"))

		require.NoError(t, console.SendLine("password"))

		require.NoError(t, console.ExpectString("Enter your age"))

		require.NoError(t, console.SendLine("15"))

		require.NoError(t, console.ExpectEOF())
	})

	// Run
	inputs := []input.Input{
		{
			ID:          "facebook.username",
			Name:        "Facebook username",
			Description: "Enter your Facebook username",
			Type:        "string",
			Kind:        "input",
		},
		{
			ID:          "facebook.password",
			Name:        "Facebook password",
			Description: "Enter your Facebook password",
			Type:        "string",
			Kind:        "hidden",
		},
		{
			ID:          "age",
			Name:        "Your age",
			Description: "Enter your age",
			Type:        "int",
			Kind:        "input",
		},
		{
			ID:          "restricted",
			Name:        "Restricted content",
			Description: "Do you want to see restricted content?",
			Type:        "bool",
			Kind:        "confirm",
			If:          "age>18",
		},
		{
			ID:          "drink",
			Name:        "Favorite drink",
			Description: "What do you like to drink?",
			Type:        "string",
			Kind:        "select",
			If:          "age>18",
			Options:     input.Options{{Value: "beer", Label: "Beer"}, {Value: "wine", Label: "Wine"}},
		},
	}

	stepsGroups := input.StepsGroups{
		{
			Description: "Group One",
			Required:    "all",
			Steps: []input.Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step One",
					Inputs:      inputs,
				},
			},
		},
	}

	f := Flags{
		Branch:       configmap.Value[string]{Value: "My Instance", SetBy: configmap.SetByDefault},
		InstanceName: configmap.Value[string]{},
		InputsFile:   configmap.Value[string]{},
	}

	output, err := AskUseTemplateOptions(t.Context(), d, projectState, stepsGroups, f)
	require.NoError(t, err)

	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{ID: 123},
		Inputs: template.InputsValues{
			{ID: "facebook.username", Value: "username"},
			{ID: "facebook.password", Value: "password"},
			{ID: "age", Value: 15},
			{ID: "restricted", Value: false, Skipped: true},
			{ID: "drink", Value: "", Skipped: true},
		},
	}, output)
}

// Some optional steps have not been selected - the output contains a default or blank value for these steps.
func TestAskUseTemplate_OptionalSteps(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())

	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(t.Context(), loadState.Options{LoadLocalState: true}, deps)
	require.NoError(t, err)

	// Run
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
							ID:          "input1",
							Name:        "input1",
							Description: "skipped + without default value",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 2",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input2",
							Name:        "input2",
							Description: "skipped + with default value",
							Type:        "string",
							Kind:        "input",
							Default:     "default value",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 3",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input3",
							Name:        "input3",
							Description: "filled in + without default value",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 4",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input4",
							Name:        "input4",
							Description: "filled in + with default value",
							Type:        "string",
							Kind:        "input",
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
	wg.Go(func() {

		require.NoError(t, console.ExpectString("Select the target branch:"))

		require.NoError(t, console.SendEnter()) // enter - Main

		require.NoError(t, console.ExpectString("Instance Name:"))

		require.NoError(t, console.SendLine("My Instance"))

		require.NoError(t, console.ExpectString("Please select which steps you want to fill."))

		require.NoError(t, console.ExpectString("Select steps:"))

		require.NoError(t, console.SendDownArrow()) // skip step 1

		require.NoError(t, console.SendDownArrow()) // skip step 2

		require.NoError(t, console.SendSpace()) // select step 3

		require.NoError(t, console.SendDownArrow()) // move to step 4

		require.NoError(t, console.SendSpace()) // select step 4

		require.NoError(t, console.SendEnter()) // confirm the selection

		require.NoError(t, console.ExpectString("Step 3"))

		require.NoError(t, console.ExpectString("input3:"))

		require.NoError(t, console.SendLine("value for input 3"))

		require.NoError(t, console.ExpectString("Step 4"))

		require.NoError(t, console.ExpectString("input4:"))

		require.NoError(t, console.SendLine("value for input 4"))

		require.NoError(t, console.ExpectEOF())
	})

	f := Flags{
		Branch:       configmap.Value[string]{Value: "My Instance", SetBy: configmap.SetByDefault},
		InstanceName: configmap.Value[string]{},
		InputsFile:   configmap.Value[string]{},
	}

	output, err := AskUseTemplateOptions(t.Context(), d, projectState, stepsGroups, f)
	require.NoError(t, err)

	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{ID: 123},
		Inputs: template.InputsValues{
			{ID: "input1", Value: "", Skipped: true},
			{ID: "input2", Value: "", Skipped: true},
			{ID: "input3", Value: "value for input 3"},
			{ID: "input4", Value: "value for input 4"},
		},
	}, output)
}

func TestAskUseTemplate_InputsFromFile(t *testing.T) {
	t.Parallel()

	// Create file with inputs
	tempDir := t.TempDir()
	inputsFile := `{"input1": "A", "input2": "B", "input4": "C"}` // input 3 is missing
	inputsFilePath := filepath.Join(tempDir, "my-inputs.json")    // nolint: forbidigo
	require.NoError(t, os.WriteFile(inputsFilePath, []byte(inputsFile), 0o600))

	d, _ := dialog.NewForTest(t, false)

	f := Flags{
		Branch:       configmap.Value[string]{Value: "123", SetBy: configmap.SetByFlag},
		InstanceName: configmap.Value[string]{Value: "My Instance", SetBy: configmap.SetByFlag},
		InputsFile:   configmap.Value[string]{Value: inputsFilePath, SetBy: configmap.SetByFlag},
	}

	deps := dependencies.NewMocked(t, t.Context())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(t.Context(), loadState.Options{LoadLocalState: true}, deps)
	require.NoError(t, err)

	// Run
	stepsGroups := input.StepsGroups{
		{
			Description: "Please select which steps you want to fill.",
			Required:    "all", // <<<<<<<<<<<
			Steps: []input.Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input1",
							Name:        "input1",
							Description: "...",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 2",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input2",
							Name:        "input2",
							Description: "...",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 3",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input3",
							Name:        "input3",
							Description: "...",
							Type:        "string",
							Kind:        "input",
							Default:     "default value",
						},
						{
							ID:          "input4",
							Name:        "input4",
							Description: "...",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
			},
		},
	}

	output, err := AskUseTemplateOptions(t.Context(), d, projectState, stepsGroups, f)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{ID: 123},
		Inputs: template.InputsValues{
			{ID: "input1", Value: "A"},
			{ID: "input2", Value: "B"},
			{ID: "input3", Value: "default value"},
			{ID: "input4", Value: "C"},
		},
	}, output)
}

func TestAskUseTemplate_InputsFromFile_InvalidStepsCount(t *testing.T) {
	t.Parallel()

	// Create file with inputs
	tempDir := t.TempDir()
	inputsFile := `{"input1": "A", "input3": "B", "input4": "C"}` // input 2 is missing
	inputsFilePath := filepath.Join(tempDir, "my-inputs.json")    // nolint: forbidigo
	require.NoError(t, os.WriteFile(inputsFilePath, []byte(inputsFile), 0o600))

	d, _ := dialog.NewForTest(t, false)

	f := Flags{
		Branch:       configmap.Value[string]{Value: "123", SetBy: configmap.SetByFlag},
		InstanceName: configmap.Value[string]{Value: "My Instance", SetBy: configmap.SetByFlag},
		InputsFile:   configmap.Value[string]{Value: inputsFilePath, SetBy: configmap.SetByFlag},
	}
	deps := dependencies.NewMocked(t, t.Context())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(t.Context(), loadState.Options{LoadLocalState: true}, deps)
	require.NoError(t, err)

	// Run
	stepsGroups := input.StepsGroups{
		{
			Description: "Please select which steps you want to fill.",
			Required:    "all", // <<<<<<<<<<<
			Steps: []input.Step{
				{
					Icon:        "common:settings",
					Name:        "Step 1",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input1",
							Name:        "input1",
							Description: "...",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 2",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input2",
							Name:        "input2",
							Description: "...",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
				{
					Icon:        "common:settings",
					Name:        "Step 3",
					Description: "Step Description",
					Inputs: []input.Input{
						{
							ID:          "input3",
							Name:        "input3",
							Description: "...",
							Type:        "string",
							Kind:        "input",
							Default:     "default value",
						},
						{
							ID:          "input4",
							Name:        "input4",
							Description: "...",
							Type:        "string",
							Kind:        "input",
						},
					},
				},
			},
		},
	}

	_, err = AskUseTemplateOptions(t.Context(), d, projectState, stepsGroups, f)
	expectedErr := `
steps group 1 "Please select which steps you want to fill." is invalid:
- all steps (3) must be selected
- number of selected steps (2) is incorrect
- in the inputs JSON file, these steps are defined:
  - Step 1, inputs: input1
  - Step 3, inputs: input3, input4
`
	require.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())
}
