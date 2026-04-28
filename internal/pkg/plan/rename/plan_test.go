package rename

import (
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestRenameAllPlan(t *testing.T) {
	t.Parallel()
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	fs := testFs(t, filesystem.Join(testDir, "..", "..", "fixtures", "local", "to-rename"))
	d := dependencies.NewMocked(t, t.Context())

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]any{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic",
	})
	require.NoError(t, err)
	getMySQLExResponder, err := httpmock.NewJsonResponder(200, map[string]any{
		"id":   "keboola.ex-db-mysql",
		"type": "extractor",
		"name": "MySQL",
	})
	require.NoError(t, err)
	d.MockedHTTPTransport().RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder.Once())
	d.MockedHTTPTransport().RegisterResponder("GET", `=~/storage/components/keboola.ex-db-mysql`, getMySQLExResponder.Once())

	// Load state
	projectState, err := d.MockedProject(fs).LoadState(t.Context(), loadState.Options{LoadLocalState: true}, d)
	require.NoError(t, err)

	// Get rename plan
	plan, err := NewPlan(projectState.State())
	require.NoError(t, err)

	// Clear manifest records before assert
	for _, action := range plan.actions {
		action.Manifest = nil
	}

	// Clear manifest ObjectManifest from actions for easier comparison
	for i := range plan.actions {
		plan.actions[i].Manifest = nil
	}

	// Assert
	assert.Equal(t, &Plan{
		actions: []model.RenameAction{
			{
				OldPath:     "my-main-branch",
				RenameFrom:  "my-main-branch",
				NewPath:     "main",
				Description: "my-main-branch -> main",
			},
			{
				OldPath:     "my-main-branch/extractor/keboola.ex-db-mysql/my-table",
				RenameFrom:  "main/extractor/keboola.ex-db-mysql/my-table",
				NewPath:     "main/extractor/keboola.ex-db-mysql/789-tables",
				Description: "main/extractor/keboola.ex-db-mysql/{my-table -> 789-tables}",
			},
			{
				OldPath:     "my-main-branch/extractor/keboola.ex-db-mysql/my-table/rows/my-row",
				RenameFrom:  "main/extractor/keboola.ex-db-mysql/789-tables/rows/my-row",
				NewPath:     "main/extractor/keboola.ex-db-mysql/789-tables/rows/12-users",
				Description: "main/extractor/keboola.ex-db-mysql/789-tables/rows/{my-row -> 12-users}",
			},
		},
	}, plan)
}

// TestRenameAllPlan_StatelessParentSkipped verifies that NewPlan does not panic
// when a config with remote state has a parent that is stateless in the registry.
// This reproduces the production scenario from PSGO-233: a scheduler config has
// a SchedulerForRelation pointing to an orchestrator whose manifest entry is
// orphaned (nil Local and nil Remote). State.All() returns the scheduler (has
// remote state) but not the orchestrator, so doUpdate reaches the parent via
// MustGet and must skip gracefully without calling LocalOrRemoteState().
func TestRenameAllPlan_StatelessParentSkipped(t *testing.T) {
	t.Parallel()
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	fs := testFs(t, filesystem.Join(testDir, "..", "..", "fixtures", "local", "to-rename"))
	d := dependencies.NewMocked(t, t.Context())

	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]any{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic",
	})
	require.NoError(t, err)
	getMySQLExResponder, err := httpmock.NewJsonResponder(200, map[string]any{
		"id":   "keboola.ex-db-mysql",
		"type": "extractor",
		"name": "MySQL",
	})
	require.NoError(t, err)
	d.MockedHTTPTransport().RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder.Once())
	d.MockedHTTPTransport().RegisterResponder("GET", `=~/storage/components/keboola.ex-db-mysql`, getMySQLExResponder.Once())

	projectState, err := d.MockedProject(fs).LoadState(t.Context(), loadState.Options{LoadLocalState: true}, d)
	require.NoError(t, err)

	// Stateless orchestrator: in the registry as an orphaned manifest entry, but
	// both Local and Remote are nil — simulates a deleted orchestrator.
	orchKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.orchestrator", ID: "orch-orphaned"}
	orchState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: orchKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("my-main-branch", "other/keboola.orchestrator/orch-orphaned"),
			},
		},
	}

	// Scheduler with remote state whose SchedulerForRelation parent key points to
	// the stateless orchestrator. State.All() will include this scheduler (it has
	// remote state), so doUpdate will recurse into the stateless orchestrator parent.
	schedulerKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.scheduler", ID: "sched-for-orch"}
	schedulerState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: schedulerKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("my-main-branch", "other/keboola.scheduler/sched-for-orch"),
			},
		},
		Remote: &model.Config{
			ConfigKey: schedulerKey,
			Relations: model.Relations{
				&model.SchedulerForRelation{
					ComponentID: "keboola.orchestrator",
					ConfigID:    "orch-orphaned",
				},
			},
		},
	}

	require.NoError(t, projectState.State().Set(orchState))
	require.NoError(t, projectState.State().Set(schedulerState))

	// NewPlan must not panic; neither orphaned object should appear in rename actions.
	plan, err := NewPlan(projectState.State())
	require.NoError(t, err)

	for _, action := range plan.actions {
		assert.NotEqual(t, action.Manifest.Key(), orchKey)
		assert.NotEqual(t, action.Manifest.Key(), schedulerKey)
	}
}

func testFs(t *testing.T, inputDir string) filesystem.Fs {
	t.Helper()

	// Create Fs
	fs := aferofs.NewMemoryFsFrom(inputDir)

	// Replace ENVs
	envs := env.Empty()
	envs.Set("LOCAL_PROJECT_ID", "12345")
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	err := testhelper.ReplaceEnvsDir(t.Context(), fs, `/`, envs)
	require.NoError(t, err)

	return fs
}
