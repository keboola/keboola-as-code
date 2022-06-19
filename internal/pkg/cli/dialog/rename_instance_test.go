package dialog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	renameOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskRenameInstance_Interactive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	d.UseMockedStorageApi()
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
	assert.NoError(t, err)
	branchKey := model.BranchKey{Id: 123}
	branchRaw, _ := projectState.LocalObjects().Get(branchKey)
	branch := branchRaw.(*model.Branch)

	now := time.Now()
	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	instanceName := "Old Name"
	repositoryName := "repo"
	tokenId := "1234"
	assert.NoError(t, branch.Metadata.UpsertTemplateInstance(now, instanceId, instanceName, templateId, repositoryName, version, tokenId, nil))
	instance, _, _ := branch.Metadata.TemplateInstance(instanceId)

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

		_, err = console.ExpectString("Instance Name")
		assert.NoError(t, err)

		_, err = console.ExpectString("(Old Name)")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("New Name")
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	opts, err := dialog.AskRenameInstance(projectState, d.Options())
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	assert.Equal(t, renameOp.Options{
		Branch:   branchKey,
		Instance: *instance,
		NewName:  "New Name",
	}, opts)
}

func TestAskRenameInstance_Noninteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _ := createDialogs(t, false)
	d := dependencies.NewTestContainer()
	d.SetFs(testfs.MinimalProjectFs(t))
	d.UseMockedStorageApi()
	projectState, err := d.LocalProjectState(loadState.Options{LoadLocalState: true})
	assert.NoError(t, err)
	branchKey := model.BranchKey{Id: 123}
	branchRaw, _ := projectState.LocalObjects().Get(branchKey)
	branch := branchRaw.(*model.Branch)

	now := time.Now()
	instanceId := "inst1"
	templateId := "tmpl1"
	version := "1.0.1"
	instanceName := "Old Name"
	repositoryName := "repo"
	tokenId := "1234"
	assert.NoError(t, branch.Metadata.UpsertTemplateInstance(now, instanceId, instanceName, templateId, repositoryName, version, tokenId, nil))
	instance, _, _ := branch.Metadata.TemplateInstance(instanceId)

	options := d.Options()
	options.Set("branch", 123)
	options.Set("instance", "inst1")
	options.Set("new-name", "New Name")
	opts, err := dialog.AskRenameInstance(projectState, options)
	assert.NoError(t, err)
	assert.Equal(t, renameOp.Options{
		Branch:   branchKey,
		Instance: *instance,
		NewName:  "New Name",
	}, opts)
}
