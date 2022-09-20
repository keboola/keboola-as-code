package dialog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskTemplateInstance_Interactive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	projectState, err := d.MockedProject(testfs.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, d)
	assert.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{Id: 123})

	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	instanceName := "Instance 1"
	repositoryName := "repo"
	tokenId := "1234"
	assert.NoError(t, branch.(*model.Branch).Metadata.UpsertTemplateInstance(time.Now(), instanceId, instanceName, templateId, repositoryName, version, tokenId, nil))

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Select branch:")
		assert.NoError(t, err)

		assert.NoError(t, console.SendEnter()) // enter - Main

		_, err = console.ExpectString("Select template instance:")
		assert.NoError(t, err)

		assert.NoError(t, console.SendEnter()) // enter - tmpl1

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	branchKey, instance, err := dialog.AskTemplateInstance(projectState, d.Options())
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	assert.Equal(t, model.BranchKey{Id: 123}, branchKey)
	assert.Equal(t, instanceId, instance.InstanceId)
}

func TestAskTemplateInstance_Noninteractive_InvalidInstance(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _ := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	projectState, err := d.MockedProject(testfs.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, d)
	assert.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{Id: 123})

	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	instanceName := "Instance 1"
	repositoryName := "repo"
	tokenId := "1234"
	assert.NoError(t, branch.(*model.Branch).Metadata.UpsertTemplateInstance(time.Now(), instanceId, instanceName, templateId, repositoryName, version, tokenId, nil))

	options := d.Options()
	options.Set("branch", 123)
	options.Set("instance", "inst2")
	_, _, err = dialog.AskTemplateInstance(projectState, options)
	assert.Error(t, err)
	assert.Equal(t, `template instance "inst2" was not found in branch "Main"`, err.Error())
}

func TestAskTemplateInstance_Noninteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _ := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	projectState, err := d.MockedProject(testfs.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, d)
	assert.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{Id: 123})

	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	instanceName := "Instance 1"
	repositoryName := "repo"
	tokenId := "1234"
	assert.NoError(t, branch.(*model.Branch).Metadata.UpsertTemplateInstance(time.Now(), instanceId, instanceName, templateId, repositoryName, version, tokenId, nil))

	options := d.Options()
	options.Set("branch", 123)
	options.Set("instance", "inst1")
	_, _, err = dialog.AskTemplateInstance(projectState, options)
	assert.NoError(t, err)
}
