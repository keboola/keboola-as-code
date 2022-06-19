package dialog_test

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

// If condition for restricted input is met by setting the age above the limit.
func TestAskUseTemplate_ShowIfMet(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	d.UseMockedStorageApi()
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
	assert.NoError(t, err)

	// Set fake file editor
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Select the target branch:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = console.ExpectString("Instance Name:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("My Instance")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your Facebook username")
		assert.NoError(t, err)

		_, err = console.ExpectString("Facebook username")
		assert.NoError(t, err)

		// username can contain alphanum only
		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("u-s")
		assert.NoError(t, err)

		_, err = console.ExpectString("username can only contain alphanumeric characters")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(strings.Repeat(Backspace, 3)) // remove "u-s"
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("username")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your Facebook password")
		assert.NoError(t, err)

		_, err = console.ExpectString("Facebook password")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("password")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your age")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("text") // enter invalid string value
		assert.NoError(t, err)

		_, err = console.ExpectString(`Sorry, your reply was invalid: value "text" is not integer`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(strings.Repeat(Backspace, 4)) // remove "text"
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("25") // enter valid numeric value
		assert.NoError(t, err)

		_, err = console.ExpectString("Do you want to see restricted content?")
		assert.NoError(t, err)

		_, err = console.ExpectString("Restricted content")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("yes")
		assert.NoError(t, err)

		_, err = console.ExpectString("What do you like to drink?")
		assert.NoError(t, err)

		_, err = console.ExpectString("Favorite drink")
		assert.NoError(t, err)

		_, err = console.ExpectString("Beer")
		assert.NoError(t, err)

		_, err = console.ExpectString("Wine")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Wine
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> confirm
		assert.NoError(t, err)

		_, err = console.ExpectString("Anything stronger?")
		assert.NoError(t, err)

		_, err = console.ExpectString("Stronger drinks")
		assert.NoError(t, err)

		_, err = console.ExpectString("Rum")
		assert.NoError(t, err)

		_, err = console.ExpectString("Vodka")
		assert.NoError(t, err)

		_, err = console.ExpectString("Whiskey")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Vodka
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Whiskey
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> confirm
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	inputs := []input.Input{
		{
			Id:          "facebook.username",
			Name:        "Facebook username",
			Description: "Enter your Facebook username",
			Type:        "string",
			Kind:        "input",
			Rules:       "alphanum",
		},
		{
			Id:          "facebook.password",
			Name:        "Facebook password",
			Description: "Enter your Facebook password",
			Type:        "string",
			Kind:        "hidden",
		},
		{
			Id:          "age",
			Name:        "Your age",
			Description: "Enter your age",
			Type:        "int",
			Kind:        "input",
		},
		{
			Id:          "restricted",
			Name:        "Restricted content",
			Description: "Do you want to see restricted content?",
			Type:        "bool",
			Kind:        "confirm",
			If:          "age>18",
		},
		{
			Id:          "drink",
			Name:        "Favorite drink",
			Description: "What do you like to drink?",
			Type:        "string",
			Kind:        "select",
			If:          "age>18",
			Options:     input.Options{{Value: "beer", Label: "Beer"}, {Value: "wine", Label: "Wine"}},
		},
		{
			Id:          "drinks",
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

	output, err := dialog.AskUseTemplateOptions(projectState, stepsGroups, d.Options())
	assert.NoError(t, err)

	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{Id: 123},
		Inputs: template.InputsValues{
			{Id: "facebook.username", Value: "username"},
			{Id: "facebook.password", Value: "password"},
			{Id: "age", Value: 25},
			{Id: "restricted", Value: true},
			{Id: "drink", Value: "wine"},
			{Id: "drinks", Value: []interface{}{"rum", "whiskey"}},
		},
	}, output)
}

// If condition for restricted input is not met by setting the age below the limit and so that input is not shown to the user.
func TestAskUseTemplate_ShowIfNotMet(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	d.UseMockedStorageApi()
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
	assert.NoError(t, err)

	// Set fake file editor
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Select the target branch:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = console.ExpectString("Instance Name:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("My Instance")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your Facebook username")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("username")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your Facebook password")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("password")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your age")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("15")
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	inputs := []input.Input{
		{
			Id:          "facebook.username",
			Name:        "Facebook username",
			Description: "Enter your Facebook username",
			Type:        "string",
			Kind:        "input",
		},
		{
			Id:          "facebook.password",
			Name:        "Facebook password",
			Description: "Enter your Facebook password",
			Type:        "string",
			Kind:        "hidden",
		},
		{
			Id:          "age",
			Name:        "Your age",
			Description: "Enter your age",
			Type:        "int",
			Kind:        "input",
		},
		{
			Id:          "restricted",
			Name:        "Restricted content",
			Description: "Do you want to see restricted content?",
			Type:        "bool",
			Kind:        "confirm",
			If:          "age>18",
		},
		{
			Id:          "drink",
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

	output, err := dialog.AskUseTemplateOptions(projectState, stepsGroups, d.Options())
	assert.NoError(t, err)

	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{Id: 123},
		Inputs: template.InputsValues{
			{Id: "facebook.username", Value: "username"},
			{Id: "facebook.password", Value: "password"},
			{Id: "age", Value: 15},
			{Id: "restricted", Value: false, Skipped: true},
			{Id: "drink", Value: "", Skipped: true},
		},
	}, output)
}

// Some optional steps have not been selected - the output contains a default or blank value for these steps.
func TestAskUseTemplate_OptionalSteps(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	d.UseMockedStorageApi()
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
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
							Id:          "input1",
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
							Id:          "input2",
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
							Id:          "input3",
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
							Id:          "input4",
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
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Select the target branch:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = console.ExpectString("Instance Name:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("My Instance")
		assert.NoError(t, err)

		_, err = console.ExpectString("Please select which steps you want to fill.")
		assert.NoError(t, err)

		_, err = console.ExpectString("Select steps:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(DownArrow) // skip step 1
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(DownArrow) // skip step 2
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(Space) // select step 3
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(DownArrow) // move to step 4
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(Space) // select step 4
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(Enter) // confirm the selection
		assert.NoError(t, err)

		_, err = console.ExpectString("Step 3")
		assert.NoError(t, err)

		_, err = console.ExpectString("input3:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("value for input 3")
		assert.NoError(t, err)

		_, err = console.ExpectString("Step 4")
		assert.NoError(t, err)

		_, err = console.ExpectString("input4:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("value for input 4")
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	output, err := dialog.AskUseTemplateOptions(projectState, stepsGroups, d.Options())
	assert.NoError(t, err)

	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{Id: 123},
		Inputs: template.InputsValues{
			{Id: "input1", Value: "", Skipped: true},
			{Id: "input2", Value: "", Skipped: true},
			{Id: "input3", Value: "value for input 3"},
			{Id: "input4", Value: "value for input 4"},
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

	// Test dependencies
	dialog, _ := createDialogs(t, false)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	d.Options().Set("branch", "123") // see MinimalProjectFs
	d.Options().Set("instance-name", "My Instance")
	d.Options().Set("inputs-file", inputsFilePath)
	d.UseMockedStorageApi()
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
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
							Id:          "input1",
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
							Id:          "input2",
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
							Id:          "input3",
							Name:        "input3",
							Description: "...",
							Type:        "string",
							Kind:        "input",
							Default:     "default value",
						},
						{
							Id:          "input4",
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

	output, err := dialog.AskUseTemplateOptions(projectState, stepsGroups, d.Options())
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, useTemplate.Options{
		InstanceName: "My Instance",
		TargetBranch: model.BranchKey{Id: 123},
		Inputs: template.InputsValues{
			{Id: "input1", Value: "A"},
			{Id: "input2", Value: "B"},
			{Id: "input3", Value: "default value"},
			{Id: "input4", Value: "C"},
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

	// Test dependencies
	dialog, _ := createDialogs(t, false)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	d.Options().Set("branch", "123") // see MinimalProjectFs
	d.Options().Set("instance-name", "My Instance")
	d.Options().Set("inputs-file", inputsFilePath)
	d.UseMockedStorageApi()
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
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
							Id:          "input1",
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
							Id:          "input2",
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
							Id:          "input3",
							Name:        "input3",
							Description: "...",
							Type:        "string",
							Kind:        "input",
							Default:     "default value",
						},
						{
							Id:          "input4",
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

	_, err = dialog.AskUseTemplateOptions(projectState, stepsGroups, d.Options())
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
