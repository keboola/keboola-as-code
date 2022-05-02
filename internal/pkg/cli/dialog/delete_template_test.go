package dialog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskDeleteTemplate_Interactive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	_, httpTransport := d.UseMockedStorageApi()
	testapi.AddMockedComponents(httpTransport)
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
	assert.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{Id: 123})

	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	assert.NoError(t, branch.(*model.Branch).Metadata.AddTemplateUsage(instanceId, templateId, version))

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Select branch:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = console.ExpectString("Select template instance:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - tmpl1
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	opts, err := dialog.AskDeleteTemplateOptions(projectState, d.Options())
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	assert.Equal(t, model.BranchKey{Id: 123}, opts.Branch)
	assert.Equal(t, instanceId, opts.Instance)
}

func TestAskDeleteTemplate_Noninteractive_InvalidInstance(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _ := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	_, httpTransport := d.UseMockedStorageApi()
	testapi.AddMockedComponents(httpTransport)
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
	assert.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{Id: 123})

	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	assert.NoError(t, branch.(*model.Branch).Metadata.AddTemplateUsage(instanceId, templateId, version))

	options := d.Options()
	options.Set("branch", 123)
	options.Set("instance", "inst2")
	_, err = dialog.AskDeleteTemplateOptions(projectState, options)
	assert.Error(t, err)
	assert.Equal(t, `template instance "inst2" was not found in branch "Main"`, err.Error())
}

func TestAskDeleteTemplate_Noninteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _ := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	_, httpTransport := d.UseMockedStorageApi()
	testapi.AddMockedComponents(httpTransport)
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
	assert.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{Id: 123})

	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	assert.NoError(t, branch.(*model.Branch).Metadata.AddTemplateUsage(instanceId, templateId, version))

	options := d.Options()
	options.Set("branch", 123)
	options.Set("instance", "inst1")
	_, err = dialog.AskDeleteTemplateOptions(projectState, options)
	assert.NoError(t, err)
}
