package create

import (
	"sync"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog/templatehelper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/create"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

func TestTemplateIdsDialog_DefaultValue(t *testing.T) {
	t.Parallel()

	branch := &model.Branch{
		BranchKey: model.BranchKey{ID: 1},
		Name:      "Branch",
	}
	configs := []*model.ConfigWithRows{
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: "foo.bar", ID: "123"},
				Name:      "My Config 1",
			},
		},
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{BranchID: 1, ComponentID: "foo.bar", ID: "456"},
				Name:      "My Config 2",
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{BranchID: 1, ComponentID: "foo.bar", ConfigID: "456", ID: "1"},
					Name:         "My Row",
				},
				{
					ConfigRowKey: model.ConfigRowKey{BranchID: 1, ComponentID: "foo.bar", ConfigID: "456", ID: "2"},
					Name:         "My Row",
				},
				{
					ConfigRowKey: model.ConfigRowKey{BranchID: 1, ComponentID: "foo.bar", ConfigID: "456", ID: "3"},
					Name:         "#$%^_",
				},
			},
		},
	}

	// Expected default value
	expected := `
<!--
Please enter a human readable ID for each configuration. For example "L0-raw-data-ex".
Allowed characters: a-z, A-Z, 0-9, "-".
These IDs will be used in the template.

Please edit each line below "## Config ..." and "### Row ...".
Do not edit lines starting with "#"!
-->


## Config "My Config 1" foo.bar:123
my-config-1

## Config "My Config 2" foo.bar:456
my-config-2

### Row "My Row" foo.bar:456:1
my-row

### Row "My Row" foo.bar:456:2
my-row-001

### Row "#$%^_" foo.bar:456:3
config-row

`

	// Check default value
	d := templateIdsDialog{prompt: nopPrompt.New(), branch: branch, configs: configs}
	assert.Equal(t, expected, d.defaultValue())
}

func TestAskCreateTemplateInteractive(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	templatehelper.AddMockedObjectsResponses(deps.MockedHTTPTransport())

	// Set fake file editor
	d.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, console.ExpectString("Please enter a template public name for users."))

		require.NoError(t, console.ExpectString("Template name:"))

		require.NoError(t, console.SendLine(`My Super Template`))

		require.NoError(t, console.ExpectString("Please enter a template internal ID."))

		require.NoError(t, console.ExpectString("Template ID:"))

		require.NoError(t, console.SendEnter()) // enter - use generated default value, "my-super-template"

		require.NoError(t, console.ExpectString("Please enter a short template description."))

		require.NoError(t, console.SendEnter()) // -> start editor

		require.NoError(t, console.ExpectString("Select the source branch:"))

		require.NoError(t, console.SendEnter()) // enter - Main

		require.NoError(t, console.ExpectString("Select the configurations to include in the template:"))

		require.NoError(t, console.ExpectString("Config 1 (keboola.my-component:1)"))

		require.NoError(t, console.ExpectString("Config 2 (keboola.my-component:2)"))

		require.NoError(t, console.ExpectString("Config 3 (keboola.my-component:3)"))

		require.NoError(t, console.SendSpace()) // -> select Config 1

		require.NoError(t, console.SendDownArrow()) // -> Config 2

		require.NoError(t, console.SendDownArrow()) // -> Config 3

		require.NoError(t, console.SendSpace()) // -> select

		require.NoError(t, console.SendEnter()) // -> confirm

		require.NoError(t, console.ExpectString("Please enter a human readable ID for each config and config row."))

		require.NoError(t, console.SendEnter()) // -> start editor

		require.NoError(t, console.ExpectString("Please select which fields in the configurations should be user inputs."))

		require.NoError(t, console.SendEnter()) // -> start editor

		require.NoError(t, console.ExpectString("Please define steps and groups for user inputs specification."))

		require.NoError(t, console.SendEnter()) // -> start editor

		require.NoError(t, console.ExpectString("Please complete the user inputs specification."))

		require.NoError(t, console.SendEnter()) // -> start editor

		require.NoError(t, console.ExpectString("Select the components that are used in the templates."))

		require.NoError(t, console.SendEnter()) // enter

		require.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := AskCreateTemplateOpts(t.Context(), d, deps, Flags{})
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

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

	d, _ := dialog.NewForTest(t, false)

	deps := dependencies.NewMocked(t, t.Context())
	templatehelper.AddMockedObjectsResponses(deps.MockedHTTPTransport())

	// Flags
	f := Flags{
		ID:             configmap.NewValueWithOrigin("my-super-template", configmap.SetByFlag),
		Name:           configmap.NewValueWithOrigin("My Super Template", configmap.SetByFlag),
		Description:    configmap.NewValueWithOrigin("Full workflow to ...", configmap.SetByFlag),
		StorageAPIHost: configmap.NewValueWithOrigin("connection.keboola.com", configmap.SetByFlag),
		Branch:         configmap.NewValueWithOrigin("123", configmap.SetByFlag),
		Configs:        configmap.NewValueWithOrigin("keboola.my-component:1, keboola.my-component:3", configmap.SetByFlag),
		AllInputs:      configmap.NewValueWithOrigin(true, configmap.SetByFlag),
	}

	// Run
	opts, err := AskCreateTemplateOpts(t.Context(), d, deps, f)
	require.NoError(t, err)

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

	d, _ := dialog.NewForTest(t, false)

	deps := dependencies.NewMocked(t, t.Context())
	templatehelper.AddMockedObjectsResponses(deps.MockedHTTPTransport())

	f := Flags{
		StorageAPIHost: configmap.NewValueWithOrigin("connection.keboola.com", configmap.SetByFlag),
		ID:             configmap.NewValueWithOrigin("my-super-template", configmap.SetByFlag),
		Name:           configmap.NewValueWithOrigin("My Super Template", configmap.SetByFlag),
		Branch:         configmap.NewValueWithOrigin("123", configmap.SetByFlag),
		AllConfigs:     configmap.NewValueWithOrigin(true, configmap.SetByFlag),
		Description:    configmap.NewValueWithOrigin("Full workflow to ...", configmap.SetByDefault),
		UsedComponents: configmap.NewValueWithOrigin("", configmap.SetByDefault),
	}

	// Run
	opts, err := AskCreateTemplateOpts(t.Context(), d, deps, f)
	require.NoError(t, err)

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
