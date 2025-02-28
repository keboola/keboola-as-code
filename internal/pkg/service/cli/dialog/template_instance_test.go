package dialog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskTemplateInstance_Interactive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMocked(t, t.Context())
	projectState, err := d.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, d)
	require.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{ID: 123})

	instanceID := "inst1"
	templateID := "tmpl1"
	version := "1.0.1"
	instanceName := "Instance 1"
	repositoryName := "repo"
	tokenID := "1234"
	require.NoError(t, branch.(*model.Branch).Metadata.UpsertTemplateInstance(time.Now(), instanceID, instanceName, templateID, repositoryName, version, tokenID, nil))

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, console.ExpectString("Select branch:"))

		require.NoError(t, console.SendEnter()) // enter - Main

		require.NoError(t, console.ExpectString("Select template instance:"))

		require.NoError(t, console.SendEnter()) // enter - tmpl1

		require.NoError(t, console.ExpectEOF())
	}()

	// Run
	branchKey, instance, err := dialog.AskTemplateInstance(projectState, configmap.NewValue(branch.String()), configmap.NewValue(instanceID))
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	assert.Equal(t, model.BranchKey{ID: 123}, branchKey)
	assert.Equal(t, instanceID, instance.InstanceID)
}

func TestAskTemplateInstance_Noninteractive_InvalidInstance(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _ := createDialogs(t, true)
	d := dependencies.NewMocked(t, t.Context())
	projectState, err := d.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, d)
	require.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{ID: 123})

	instanceID := "inst1"
	templateID := "tmpl1"
	version := "1.0.1"
	instanceName := "Instance 1"
	repositoryName := "repo"
	tokenID := "1234"
	require.NoError(t, branch.(*model.Branch).Metadata.UpsertTemplateInstance(time.Now(), instanceID, instanceName, templateID, repositoryName, version, tokenID, nil))

	_, _, err = dialog.AskTemplateInstance(projectState, configmap.NewValueWithOrigin("123", configmap.SetByFlag), configmap.NewValueWithOrigin("inst2", configmap.SetByFlag))
	require.Error(t, err)
	assert.Equal(t, `template instance "inst2" was not found in branch "Main"`, err.Error())
}

func TestAskTemplateInstance_Noninteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _ := createDialogs(t, true)
	d := dependencies.NewMocked(t, t.Context())
	projectState, err := d.MockedProject(fixtures.MinimalProjectFs(t)).LoadState(loadState.Options{LoadLocalState: true}, d)
	require.NoError(t, err)
	branch, _ := projectState.LocalObjects().Get(model.BranchKey{ID: 123})

	instanceID := "inst1"
	templateID := "tmpl1"
	version := "1.0.1"
	instanceName := "Instance 1"
	repositoryName := "repo"
	tokenID := "1234"
	require.NoError(t, branch.(*model.Branch).Metadata.UpsertTemplateInstance(time.Now(), instanceID, instanceName, templateID, repositoryName, version, tokenID, nil))

	_, _, err = dialog.AskTemplateInstance(projectState, configmap.NewValueWithOrigin("123", configmap.SetByFlag), configmap.NewValueWithOrigin(instanceID, configmap.SetByFlag))
	require.NoError(t, err)
}
