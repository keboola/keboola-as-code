package dialog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/create"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

func TestAskCreateTemplateInteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	_, httpTransport := d.UseMockedStorageApi()
	addMockedObjectsResponses(httpTransport)

	// Set fake file editor
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Please enter Keboola Storage API host, eg. \"connection.keboola.com\".")
		assert.NoError(t, err)

		_, err = console.ExpectString("API host: ")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`foo.bar.com`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
		assert.NoError(t, err)

		_, err = console.ExpectString("API token: ")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`my-secret-token`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Please enter a template public name for users.")
		assert.NoError(t, err)

		_, err = console.ExpectString("Template name:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`My Super Template`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Please enter a template internal ID.")
		assert.NoError(t, err)

		_, err = console.ExpectString("Template ID:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - use generated default value, "my-super-template"
		assert.NoError(t, err)

		_, err = console.ExpectString("Please enter a short template description.")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> start editor
		assert.NoError(t, err)

		_, err = console.ExpectString("Select the source branch:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = console.ExpectString("Select the configurations to include in the template:")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 1 (keboola.my-component:1)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 2 (keboola.my-component:2)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 3 (keboola.my-component:3)")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select Config 1
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Config 2
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Config 3
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> confirm
		assert.NoError(t, err)

		_, err = console.ExpectString("Please enter a human readable ID for each config and config row.")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> start editor
		assert.NoError(t, err)

		_, err = console.ExpectString("Please select which fields in the configurations should be user inputs.")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> start editor
		assert.NoError(t, err)

		_, err = console.ExpectString("Please define steps and groups for user inputs specification.")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> start editor
		assert.NoError(t, err)

		_, err = console.ExpectString("Please complete the user inputs specification.")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> start editor
		assert.NoError(t, err)

		_, err = console.ExpectString("Select the components that are used in the templates.")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	opts, err := dialog.AskCreateTemplateOpts(d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createTemplate.Options{
		Id:           `my-super-template`,
		Name:         `My Super Template`,
		Description:  `Full workflow to ...`,
		SourceBranch: model.BranchKey{Id: 123},
		Configs: []create.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `keboola.my-component`,
					Id:          `1`,
				},
				TemplateId: `config-1`,
				Inputs: []create.InputDef{
					{
						InputId: "my-component-password",
						Path:    orderedmap.KeyFromStr("parameters.#password"),
					},
				},
				Rows: []create.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchId:    123,
							ComponentId: `keboola.my-component`,
							ConfigId:    `1`,
							Id:          `456`,
						},
						TemplateId: `my-row`,
					},
				},
			},
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `keboola.my-component`,
					Id:          `3`,
				},
				TemplateId: `config-3`,
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
								Id:   "my-component-password",
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
	dialog, _ := createDialogs(t, false)
	d := dependencies.NewTestContainer()
	_, httpTransport := d.UseMockedStorageApi()
	addMockedObjectsResponses(httpTransport)

	// Flags
	d.Options().Set(`storage-api-host`, `connection.keboola.com`)
	d.Options().Set(`storage-api-token`, `my-secret`)
	d.Options().Set(`name`, `My Super Template`)
	d.Options().Set(`id`, `my-super-template`)
	d.Options().Set(`branch`, `123`)
	d.Options().Set(`configs`, `keboola.my-component:1, keboola.my-component:3`)
	d.Options().Set(`all-inputs`, true)

	// Run
	opts, err := dialog.AskCreateTemplateOpts(d)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, createTemplate.Options{
		Id:           `my-super-template`,
		Name:         `My Super Template`,
		Description:  `Full workflow to ...`,
		SourceBranch: model.BranchKey{Id: 123},
		Configs: []create.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `keboola.my-component`,
					Id:          `1`,
				},
				TemplateId: `config-1`,
				Inputs: []create.InputDef{
					{
						InputId: "my-component-password",
						Path:    orderedmap.KeyFromStr("parameters.#password"),
					},
					{
						InputId: "my-component-int",
						Path:    orderedmap.KeyFromStr("parameters.int"),
					},
					{
						InputId: "my-component-string",
						Path:    orderedmap.KeyFromStr("parameters.string"),
					},
				},
				Rows: []create.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchId:    123,
							ComponentId: `keboola.my-component`,
							ConfigId:    `1`,
							Id:          `456`,
						},
						TemplateId: `my-row`,
					},
				},
			},
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `keboola.my-component`,
					Id:          `3`,
				},
				TemplateId: `config-3`,
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
								Id:   "my-component-password",
								Name: "Password",
								Type: input.TypeString,
								Kind: input.KindHidden,
							},
							{
								Id:      "my-component-int",
								Name:    "Int",
								Type:    input.TypeInt,
								Kind:    input.KindInput,
								Default: 123,
							},
							{
								Id:      "my-component-string",
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
	dialog, _ := createDialogs(t, false)
	d := dependencies.NewTestContainer()
	_, httpTransport := d.UseMockedStorageApi()
	addMockedObjectsResponses(httpTransport)

	// Flags
	d.Options().Set(`storage-api-host`, `connection.keboola.com`)
	d.Options().Set(`storage-api-token`, `my-secret`)
	d.Options().Set(`name`, `My Super Template`)
	d.Options().Set(`id`, `my-super-template`)
	d.Options().Set(`branch`, `123`)
	d.Options().Set(`all-configs`, true) // <<<<<<<<<<<<<<<<

	// Run
	opts, err := dialog.AskCreateTemplateOpts(d)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, createTemplate.Options{
		Id:           `my-super-template`,
		Name:         `My Super Template`,
		Description:  `Full workflow to ...`,
		SourceBranch: model.BranchKey{Id: 123},
		Configs: []create.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `keboola.my-component`,
					Id:          `1`,
				},
				TemplateId: `config-1`,
				Inputs: []create.InputDef{
					{
						InputId: "my-component-password",
						Path:    orderedmap.KeyFromStr("parameters.#password"),
					},
				},
				Rows: []create.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchId:    123,
							ComponentId: `keboola.my-component`,
							ConfigId:    `1`,
							Id:          `456`,
						},
						TemplateId: `my-row`,
					},
				},
			},
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `keboola.my-component`,
					Id:          `2`,
				},
				TemplateId: `config-2`,
			},
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `keboola.my-component`,
					Id:          `3`,
				},
				TemplateId: `config-3`,
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
								Id:   "my-component-password",
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
	configJson := `
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
	json.MustDecodeString(configJson, configContent)

	branches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}
	configs := []*model.ConfigWithRows{
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{Id: "1"},
				Name:      `Config 1`,
				Content:   configContent,
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{Id: "456"},
					Name:         `My Row`,
					Content:      orderedmap.New(),
				},
			},
		},
		{Config: &model.Config{ConfigKey: model.ConfigKey{Id: "2"}, Name: `Config 2`, Content: orderedmap.New()}},
		{Config: &model.Config{ConfigKey: model.ConfigKey{Id: "3"}, Name: `Config 3`, Content: orderedmap.New()}},
	}
	components := []*model.ComponentWithConfigs{
		{
			Component: &model.Component{ComponentKey: model.ComponentKey{Id: `keboola.my-component`}, Name: `Foo Bar`},
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
