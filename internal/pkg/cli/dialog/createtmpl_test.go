package dialog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

func TestAskCreateTemplateInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	var err error
	var httpTransport *httpmock.MockTransport
	dialog, console := createDialogs(t, true)
	d := testdeps.NewDependencies()
	d.LoggerValue = log.NewNopLogger()
	d.FsValue = testfs.NewMemoryFs()
	d.StorageApiValue, httpTransport, _ = testapi.NewMockedStorageApi()
	setupCreateTemplateApiResponses(httpTransport)

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

		_, err = console.ExpectString("Select the source branch:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = console.ExpectString("Select the configurations to include in the template:")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 1 (foo.bar:1)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 2 (foo.bar:2)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Config 3 (foo.bar:3)")
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

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> start editor
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
		Id:   `my-super-template`,
		Name: `My Super Template`,
		Configs: []createTemplate.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `foo.bar`,
					Id:          `1`,
				},
				TemplateId: `config-1`,
				Rows: []createTemplate.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchId:    123,
							ComponentId: `foo.bar`,
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
					ComponentId: `foo.bar`,
					Id:          `3`,
				},
				TemplateId: `config-3`,
			},
		},
	}, opts)
}

func TestAskCreateTemplateNonInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	var err error
	var httpTransport *httpmock.MockTransport
	dialog, _ := createDialogs(t, false)
	d := testdeps.NewDependencies()
	d.LoggerValue = log.NewNopLogger()
	d.FsValue = testfs.NewMemoryFs()
	d.StorageApiValue, httpTransport, _ = testapi.NewMockedStorageApi()
	setupCreateTemplateApiResponses(httpTransport)

	// Flags
	d.Options().Set(`storage-api-host`, `connection.keboola.com`)
	d.Options().Set(`storage-api-token`, `my-secret`)
	d.Options().Set(`name`, `My Super Template`)
	d.Options().Set(`id`, `my-super-template`)
	d.Options().Set(`branch`, `123`)
	d.Options().Set(`configs`, `foo.bar:1, foo.bar:3`)

	// Run
	opts, err := dialog.AskCreateTemplateOpts(d)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, createTemplate.Options{
		Id:   `my-super-template`,
		Name: `My Super Template`,
		Configs: []createTemplate.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `foo.bar`,
					Id:          `1`,
				},
				TemplateId: `config-1`,
				Rows: []createTemplate.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchId:    123,
							ComponentId: `foo.bar`,
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
					ComponentId: `foo.bar`,
					Id:          `3`,
				},
				TemplateId: `config-3`,
			},
		},
	}, opts)
}

func TestAskCreateTemplateAllConfigs(t *testing.T) {
	t.Parallel()

	// Dependencies
	var err error
	var httpTransport *httpmock.MockTransport
	dialog, _ := createDialogs(t, false)
	d := testdeps.NewDependencies()
	d.LoggerValue = log.NewNopLogger()
	d.FsValue = testfs.NewMemoryFs()
	d.StorageApiValue, httpTransport, _ = testapi.NewMockedStorageApi()
	setupCreateTemplateApiResponses(httpTransport)

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
		Id:   `my-super-template`,
		Name: `My Super Template`,
		Configs: []createTemplate.ConfigDef{
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `foo.bar`,
					Id:          `1`,
				},
				TemplateId: `config-1`,
				Rows: []createTemplate.ConfigRowDef{
					{
						Key: model.ConfigRowKey{
							BranchId:    123,
							ComponentId: `foo.bar`,
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
					ComponentId: `foo.bar`,
					Id:          `2`,
				},
				TemplateId: `config-2`,
			},
			{
				Key: model.ConfigKey{
					BranchId:    123,
					ComponentId: `foo.bar`,
					Id:          `3`,
				},
				TemplateId: `config-3`,
			},
		},
	}, opts)
}

func setupCreateTemplateApiResponses(httpTransport *httpmock.MockTransport) {
	branches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}
	configs := []*model.ConfigWithRows{
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{Id: "1"},
				Name:      `Config 1`,
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{Id: "456"},
					Name:         `My Row`,
				},
			},
		},
		{Config: &model.Config{ConfigKey: model.ConfigKey{Id: "2"}, Name: `Config 2`}},
		{Config: &model.Config{ConfigKey: model.ConfigKey{Id: "3"}, Name: `Config 3`}},
	}
	components := []*model.ComponentWithConfigs{
		{
			Component: &model.Component{ComponentKey: model.ComponentKey{Id: `foo.bar`}, Name: `Foo Bar`},
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
