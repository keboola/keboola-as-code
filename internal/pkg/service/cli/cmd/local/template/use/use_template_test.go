package use

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

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

	deps := dependencies.NewMocked(t, context.Background())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, deps)
	assert.NoError(t, err)

	// Set fake file editor
	d.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select the target branch:"))

		assert.NoError(t, console.SendEnter()) // enter - Main

		assert.NoError(t, console.ExpectString("Instance Name:"))

		assert.NoError(t, console.SendLine("My Instance"))

		assert.NoError(t, console.ExpectString("Enter your Facebook username"))

		assert.NoError(t, console.ExpectString("Facebook username"))

		// username can contain alphanum only
		assert.NoError(t, console.SendLine("u-s"))

		assert.NoError(t, console.ExpectString(`Facebook username can only contain alphanumeric characters`))

		assert.NoError(t, console.Send(strings.Repeat(Backspace, 3))) // remove "u-s"

		assert.NoError(t, console.SendLine("username"))

		assert.NoError(t, console.ExpectString("Enter your Facebook password"))

		assert.NoError(t, console.ExpectString("Facebook password"))

		assert.NoError(t, console.SendLine("password"))

		assert.NoError(t, console.ExpectString("Enter your age"))

		assert.NoError(t, console.SendLine("text")) // enter invalid string value

		assert.NoError(t, console.ExpectString(`Sorry, your reply was invalid: value "text" is not integer`))

		assert.NoError(t, console.Send(strings.Repeat(Backspace, 4))) // remove "text"

		assert.NoError(t, console.SendLine("25")) // enter valid numeric value

		assert.NoError(t, console.ExpectString("Do you want to see restricted content?"))

		assert.NoError(t, console.ExpectString("Restricted content"))

		assert.NoError(t, console.SendLine("yes"))

		assert.NoError(t, console.ExpectString("What do you like to drink?"))

		assert.NoError(t, console.ExpectString("Favorite drink"))

		assert.NoError(t, console.ExpectString("Beer"))

		assert.NoError(t, console.ExpectString("Wine"))

		assert.NoError(t, console.SendDownArrow()) // -> Wine

		assert.NoError(t, console.SendSpace()) // -> select

		assert.NoError(t, console.SendEnter()) // -> confirm

		assert.NoError(t, console.ExpectString("Anything stronger?"))

		assert.NoError(t, console.ExpectString("Stronger drinks"))

		assert.NoError(t, console.ExpectString("Rum"))

		assert.NoError(t, console.ExpectString("Vodka"))

		assert.NoError(t, console.ExpectString("Whiskey"))

		assert.NoError(t, console.SendSpace()) // -> select

		assert.NoError(t, console.SendDownArrow()) // -> Vodka

		assert.NoError(t, console.SendDownArrow()) // -> Whiskey

		assert.NoError(t, console.SendSpace()) // -> select

		assert.NoError(t, console.SendEnter()) // -> confirm

		assert.NoError(t, console.ExpectEOF())
	}()

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

	output, err := AskUseTemplateOptions(context.Background(), d, projectState, stepsGroups, f)
	assert.NoError(t, err)

	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

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

	deps := dependencies.NewMocked(t, context.Background())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, deps)
	assert.NoError(t, err)

	// Set fake file editor
	d.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select the target branch:"))

		assert.NoError(t, console.SendEnter()) // enter - Main

		assert.NoError(t, console.ExpectString("Instance Name:"))

		assert.NoError(t, console.SendLine("My Instance"))

		assert.NoError(t, console.ExpectString("Enter your Facebook username"))

		assert.NoError(t, console.SendLine("username"))

		assert.NoError(t, console.ExpectString("Enter your Facebook password"))

		assert.NoError(t, console.SendLine("password"))

		assert.NoError(t, console.ExpectString("Enter your age"))

		assert.NoError(t, console.SendLine("15"))

		assert.NoError(t, console.ExpectEOF())
	}()

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

	output, err := AskUseTemplateOptions(context.Background(), d, projectState, stepsGroups, f)
	assert.NoError(t, err)

	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

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

	deps := dependencies.NewMocked(t, context.Background())

	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, deps)
	assert.NoError(t, err)

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
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select the target branch:"))

		assert.NoError(t, console.SendEnter()) // enter - Main

		assert.NoError(t, console.ExpectString("Instance Name:"))

		assert.NoError(t, console.SendLine("My Instance"))

		assert.NoError(t, console.ExpectString("Please select which steps you want to fill."))

		assert.NoError(t, console.ExpectString("Select steps:"))

		assert.NoError(t, console.SendDownArrow()) // skip step 1

		assert.NoError(t, console.SendDownArrow()) // skip step 2

		assert.NoError(t, console.SendSpace()) // select step 3

		assert.NoError(t, console.SendDownArrow()) // move to step 4

		assert.NoError(t, console.SendSpace()) // select step 4

		assert.NoError(t, console.SendEnter()) // confirm the selection

		assert.NoError(t, console.ExpectString("Step 3"))

		assert.NoError(t, console.ExpectString("input3:"))

		assert.NoError(t, console.SendLine("value for input 3"))

		assert.NoError(t, console.ExpectString("Step 4"))

		assert.NoError(t, console.ExpectString("input4:"))

		assert.NoError(t, console.SendLine("value for input 4"))

		assert.NoError(t, console.ExpectEOF())
	}()

	f := Flags{
		Branch:       configmap.Value[string]{Value: "My Instance", SetBy: configmap.SetByDefault},
		InstanceName: configmap.Value[string]{},
		InputsFile:   configmap.Value[string]{},
	}

	output, err := AskUseTemplateOptions(context.Background(), d, projectState, stepsGroups, f)
	assert.NoError(t, err)

	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

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
	assert.NoError(t, os.WriteFile(inputsFilePath, []byte(inputsFile), 0o600))

	d, _ := dialog.NewForTest(t, false)

	f := Flags{
		Branch:       configmap.Value[string]{Value: "123", SetBy: configmap.SetByFlag},
		InstanceName: configmap.Value[string]{Value: "My Instance", SetBy: configmap.SetByFlag},
		InputsFile:   configmap.Value[string]{Value: inputsFilePath, SetBy: configmap.SetByFlag},
	}

	deps := dependencies.NewMocked(t, context.Background())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, deps)
	assert.NoError(t, err)

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

	output, err := AskUseTemplateOptions(context.Background(), d, projectState, stepsGroups, f)
	assert.NoError(t, err)

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
	assert.NoError(t, os.WriteFile(inputsFilePath, []byte(inputsFile), 0o600))

	d, _ := dialog.NewForTest(t, false)

	f := Flags{
		Branch:       configmap.Value[string]{Value: "123", SetBy: configmap.SetByFlag},
		InstanceName: configmap.Value[string]{Value: "My Instance", SetBy: configmap.SetByFlag},
		InputsFile:   configmap.Value[string]{Value: inputsFilePath, SetBy: configmap.SetByFlag},
	}
	deps := dependencies.NewMocked(t, context.Background())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, deps)
	assert.NoError(t, err)

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

	_, err = AskUseTemplateOptions(context.Background(), d, projectState, stepsGroups, f)
	expectedErr := `
steps group 1 "Please select which steps you want to fill." is invalid:
- all steps (3) must be selected
- number of selected steps (2) is incorrect
- in the inputs JSON file, these steps are defined:
  - Step 1, inputs: input1
  - Step 3, inputs: input3, input4
`
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expectedErr, "\n"), err.Error())
}
