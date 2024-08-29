package rename

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	renameOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskRenameInstance_Interactive(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps, _ := dependencies.NewMocked(t, context.Background())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, deps)
	assert.NoError(t, err)
	branchKey := model.BranchKey{ID: 123}
	branchRaw, _ := projectState.LocalObjects().Get(branchKey)
	branch := branchRaw.(*model.Branch)

	now := time.Now()
	instanceID := "inst1"
	templateID := "tmpl1"
	version := "1.0.1"
	instanceName := "Old Name"
	repositoryName := "repo"
	tokenID := "1234"
	assert.NoError(t, branch.Metadata.UpsertTemplateInstance(now, instanceID, instanceName, templateID, repositoryName, version, tokenID, nil))
	instance, _, _ := branch.Metadata.TemplateInstance(instanceID)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select branch:"))

		assert.NoError(t, console.SendEnter()) // enter - Main

		assert.NoError(t, console.ExpectString("Select template instance:"))

		assert.NoError(t, console.SendEnter()) // enter - tmpl1

		assert.NoError(t, console.ExpectString("Instance Name"))

		assert.NoError(t, console.ExpectString("(Old Name)"))

		assert.NoError(t, console.SendLine("New Name"))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := AskRenameInstance(projectState, d, Flags{})
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

	d, _ := dialog.NewForTest(t, false)

	deps, _ := dependencies.NewMocked(t, context.Background())
	projectState, err := deps.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, deps)
	assert.NoError(t, err)
	branchKey := model.BranchKey{ID: 123}
	branchRaw, _ := projectState.LocalObjects().Get(branchKey)
	branch := branchRaw.(*model.Branch)

	now := time.Now()
	instanceID := "inst1"
	templateID := "tmpl1"
	version := "1.0.1"
	instanceName := "Old Name"
	repositoryName := "repo"
	tokenID := "1234"
	assert.NoError(t, branch.Metadata.UpsertTemplateInstance(now, instanceID, instanceName, templateID, repositoryName, version, tokenID, nil))
	instance, _, _ := branch.Metadata.TemplateInstance(instanceID)

	f := Flags{
		Branch:   configmap.Value[string]{Value: branch.Name, SetBy: configmap.SetByFlag},
		Instance: configmap.Value[string]{Value: "inst1", SetBy: configmap.SetByFlag},
		NewName:  configmap.Value[string]{Value: "New Name", SetBy: configmap.SetByFlag},
	}
	opts, err := AskRenameInstance(projectState, d, f)
	assert.NoError(t, err)
	assert.Equal(t, renameOp.Options{
		Branch:   branchKey,
		Instance: *instance,
		NewName:  "New Name",
	}, opts)
}
