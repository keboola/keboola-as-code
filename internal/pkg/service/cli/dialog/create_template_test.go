package dialog_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	createTemp "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/create"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

func TestAskCreateTemplateInteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _, console := createDialogs(t, true)
	d := dependencies.NewMocked(t)
	addMockedObjectsResponses(d.MockedHTTPTransport())

	// Set fake file editor
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Please enter a template public name for users."))

		assert.NoError(t, console.ExpectString("Template name:"))

		assert.NoError(t, console.SendLine(`My Super Template`))

		assert.NoError(t, console.ExpectString("Please enter a template internal ID."))

		assert.NoError(t, console.ExpectString("Template ID:"))

		assert.NoError(t, console.SendEnter()) // enter - use generated default value, "my-super-template"

		assert.NoError(t, console.ExpectString("Please enter a short template description."))

		assert.NoError(t, console.SendEnter()) // -> start editor

		assert.NoError(t, console.ExpectString("Select the source branch:"))

		assert.NoError(t, console.SendEnter()) // enter - Main

		assert.NoError(t, console.ExpectString("Select the configurations to include in the template:"))

		assert.NoError(t, console.ExpectString("Config 1 (keboola.my-component:1)"))

		assert.NoError(t, console.ExpectString("Config 2 (keboola.my-component:2)"))

		assert.NoError(t, console.ExpectString("Config 3 (keboola.my-component:3)"))

		assert.NoError(t, console.SendSpace()) // -> select Config 1

		assert.NoError(t, console.SendDownArrow()) // -> Config 2

		assert.NoError(t, console.SendDownArrow()) // -> Config 3

		assert.NoError(t, console.SendSpace()) // -> select

		assert.NoError(t, console.SendEnter()) // -> confirm

		assert.NoError(t, console.ExpectString("Please enter a human readable ID for each config and config row."))

		assert.NoError(t, console.SendEnter()) // -> start editor

		assert.NoError(t, console.ExpectString("Please select which fields in the configurations should be user inputs."))

		assert.NoError(t, console.SendEnter()) // -> start editor

		assert.NoError(t, console.ExpectString("Please define steps and groups for user inputs specification."))

		assert.NoError(t, console.SendEnter()) // -> start editor

		assert.NoError(t, console.ExpectString("Please complete the user inputs specification."))

		assert.NoError(t, console.SendEnter()) // -> start editor

		assert.NoError(t, console.ExpectString("Select the components that are used in the templates."))

		assert.NoError(t, console.SendEnter()) // enter

		assert.NoError(t, console.ExpectEOF())
	}()

	// flags
	f := createTemp.Flags{
		ID:          configmap.NewValue("my-super-template"),
		Name:        configmap.NewValue("My Super Template"),
		Branch:      configmap.NewValue("123"),
		Configs:     configmap.NewValue("Config 1 (keboola.my-component:1)"),
		Description: configmap.NewValue("Full workflow to ..."),
	}
	// Run
	opts, err := createTemp.AskCreateTemplateOpts(context.Background(), dialog, d, f)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createTemplate.Options{
		ID:           `my-super-template`,
		Name:         `My Super Template`,
		Description:  `Full workflow to ...`,
		SourceBranch: model.BranchKey{ID: 123},
		Configs: []create.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchID:    123,
					ComponentID: `keboola.my-component`,
					ID:          `1`,
				},
				TemplateID: `config-1`,
				Inputs: []create.InputDef{
					{
						InputID: "my-component-password",
						Path:    orderedmap.PathFromStr("parameters.#password"),
					},
				},
				Rows: []create.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchID:    123,
							ComponentID: `keboola.my-component`,
							ConfigID:    `1`,
							ID:          `456`,
						},
						TemplateID: `my-row`,
					},
				},
			},
			{
				Key: model.ConfigKey{
					BranchID:    123,
					ComponentID: `keboola.my-component`,
					ID:          `3`,
				},
				TemplateID: `config-3`,
			},
		},
		StepsGroups: input.StepsGroups{
			{
				Description: "Default Group",
				Required:    "all",
				Steps: []input.Step{
					{
						Icon:        "common:settings",
						Name:        "Default Step",
						Description: "Default Step",
						Inputs: input.Inputs{
							{
								ID:   "my-component-password",
								Name: "Password",
								Type: input.TypeString,
								Kind: input.KindHidden,
							},
						},
					},
				},
			},
		},
		Components: []string{},
	}, opts)
}

func TestAskCreateTemplateNonInteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _, _ := createDialogs(t, false)
	d := dependencies.NewMocked(t)
	addMockedObjectsResponses(d.MockedHTTPTransport())

	// Flags
	// o.Set(`storage-api-host`, `connection.keboola.com`)
	// o.Set(`storage-api-token`, `my-secret`)
	// o.Set(`name`, `My Super Template`)
	// o.Set(`id`, `my-super-template`)
	// o.Set(`branch`, `123`)
	// o.Set(`configs`, `keboola.my-component:1, keboola.my-component:3`)
	// o.Set(`all-inputs`, true)

	// Flags
	f := createTemp.Flags{
		ID:             configmap.Value[string]{Value: "my-super-template", SetBy: configmap.SetByFlag},
		Name:           configmap.Value[string]{Value: "My Super Template", SetBy: configmap.SetByFlag},
		Description:    configmap.Value[string]{Value: "Full workflow to ...", SetBy: configmap.SetByFlag},
		StorageAPIHost: configmap.Value[string]{Value: "connection.keboola.com", SetBy: configmap.SetByFlag},
		Branch:         configmap.Value[string]{Value: "123", SetBy: configmap.SetByFlag},
		Configs:        configmap.Value[string]{Value: "keboola.my-component:1, keboola.my-component:3", SetBy: configmap.SetByFlag},
		UsedComponents: configmap.Value[string]{Value: "", SetBy: configmap.SetByDefault},
		// AllConfigs:     configmap.Value[bool]{Value: "my-super-template", SetBy: configmap.SetByFlag},
		AllInputs: configmap.Value[bool]{Value: true, SetBy: configmap.SetByFlag},
	}

	// Run
	opts, err := createTemp.AskCreateTemplateOpts(context.Background(), dialog, d, f)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, createTemplate.Options{
		ID:           `my-super-template`,
		Name:         `My Super Template`,
		Description:  `Full workflow to ...`,
		SourceBranch: model.BranchKey{ID: 123},
		Configs: []create.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchID:    123,
					ComponentID: `keboola.my-component`,
					ID:          `1`,
				},
				TemplateID: `config-1`,
				Inputs: []create.InputDef{
					{
						InputID: "my-component-password",
						Path:    orderedmap.PathFromStr("parameters.#password"),
					},
					{
						InputID: "my-component-int",
						Path:    orderedmap.PathFromStr("parameters.int"),
					},
					{
						InputID: "my-component-string",
						Path:    orderedmap.PathFromStr("parameters.string"),
					},
				},
				Rows: []create.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchID:    123,
							ComponentID: `keboola.my-component`,
							ConfigID:    `1`,
							ID:          `456`,
						},
						TemplateID: `my-row`,
					},
				},
			},
			{
				Key: model.ConfigKey{
					BranchID:    123,
					ComponentID: `keboola.my-component`,
					ID:          `3`,
				},
				TemplateID: `config-3`,
			},
		},
		StepsGroups: input.StepsGroups{
			{
				Description: "Default Group",
				Required:    "all",
				Steps: []input.Step{
					{
						Icon:        "common:settings",
						Name:        "Default Step",
						Description: "Default Step",
						Inputs: input.Inputs{
							{
								ID:   "my-component-password",
								Name: "Password",
								Type: input.TypeString,
								Kind: input.KindHidden,
							},
							{
								ID:      "my-component-int",
								Name:    "Int",
								Type:    input.TypeInt,
								Kind:    input.KindInput,
								Default: 123,
							},
							{
								ID:      "my-component-string",
								Name:    "String",
								Type:    input.TypeString,
								Kind:    input.KindInput,
								Default: "my string",
							},
						},
					},
				},
			},
		},
		Components: []string{},
	}, opts)
}

func TestAskCreateTemplateAllConfigs(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _, _ := createDialogs(t, false)
	d := dependencies.NewMocked(t)
	addMockedObjectsResponses(d.MockedHTTPTransport())

	f := createTemp.Flags{
		StorageAPIHost: configmap.Value[string]{Value: "connection.keboola.com", SetBy: configmap.SetByFlag},
		ID:             configmap.Value[string]{Value: "my-super-template", SetBy: configmap.SetByFlag},
		Name:           configmap.Value[string]{Value: "My Super Template", SetBy: configmap.SetByFlag},
		Branch:         configmap.Value[string]{Value: "123", SetBy: configmap.SetByFlag},
		AllConfigs:     configmap.Value[bool]{Value: true, SetBy: configmap.SetByFlag},
		Description:    configmap.NewValue("Full workflow to ..."),
		UsedComponents: configmap.NewValue(""),
	}

	// Run
	opts, err := createTemp.AskCreateTemplateOpts(context.Background(), dialog, d, f)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, createTemplate.Options{
		ID:           `my-super-template`,
		Name:         `My Super Template`,
		Description:  `Full workflow to ...`,
		SourceBranch: model.BranchKey{ID: 123},
		Configs: []create.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchID:    123,
					ComponentID: `keboola.my-component`,
					ID:          `1`,
				},
				TemplateID: `config-1`,
				Inputs: []create.InputDef{
					{
						InputID: "my-component-password",
						Path:    orderedmap.PathFromStr("parameters.#password"),
					},
				},
				Rows: []create.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchID:    123,
							ComponentID: `keboola.my-component`,
							ConfigID:    `1`,
							ID:          `456`,
						},
						TemplateID: `my-row`,
					},
				},
			},
			{
				Key: model.ConfigKey{
					BranchID:    123,
					ComponentID: `keboola.my-component`,
					ID:          `2`,
				},
				TemplateID: `config-2`,
			},
			{
				Key: model.ConfigKey{
					BranchID:    123,
					ComponentID: `keboola.my-component`,
					ID:          `3`,
				},
				TemplateID: `config-3`,
			},
		},
		StepsGroups: input.StepsGroups{
			{
				Description: "Default Group",
				Required:    "all",
				Steps: []input.Step{
					{
						Icon:        "common:settings",
						Name:        "Default Step",
						Description: "Default Step",
						Inputs: input.Inputs{
							{
								ID:   "my-component-password",
								Name: "Password",
								Type: input.TypeString,
								Kind: input.KindHidden,
							},
						},
					},
				},
			},
		},
		Components: []string{},
	}, opts)
}

func addMockedObjectsResponses(httpTransport *httpmock.MockTransport) {
	configJSON := `
{
  "storage": {
    "foo": "bar"
  },
  "parameters": {
    "string": "my string",
    "#password": "my password",
    "int": 123
  }
}
`
	configContent := orderedmap.New()
	json.MustDecodeString(configJSON, configContent)

	branches := []*model.Branch{{BranchKey: model.BranchKey{ID: 123}, Name: "Main", IsDefault: true}}
	configs := []*keboola.ConfigWithRows{
		{
			Config: &keboola.Config{
				ConfigKey: keboola.ConfigKey{ID: "1"},
				Name:      `Config 1`,
				Content:   configContent,
			},
			Rows: []*keboola.ConfigRow{
				{
					ConfigRowKey: keboola.ConfigRowKey{ID: "456"},
					Name:         `My Row`,
					Content:      orderedmap.New(),
				},
			},
		},
		{Config: &keboola.Config{ConfigKey: keboola.ConfigKey{ID: "2"}, Name: `Config 2`, Content: orderedmap.New()}},
		{Config: &keboola.Config{ConfigKey: keboola.ConfigKey{ID: "3"}, Name: `Config 3`, Content: orderedmap.New()}},
	}
	components := []*keboola.ComponentWithConfigs{
		{
			Component: keboola.Component{ComponentKey: keboola.ComponentKey{ID: `keboola.my-component`}, Name: `Foo Bar`},
			Configs:   configs,
		},
	}
	httpTransport.RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)
	httpTransport.RegisterResponder(
		"GET", `=~/storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, components),
	)
}
