package state_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestValidateState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create state
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_PROJECT_ID", `123`)
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", `123`)
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", `456`)

	// Container
	d := dependencies.NewMocked(t, ctx)
	state := d.MockedState()

	// Mocked component response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]any{
		"id":   "keboola.foo",
		"type": "writer",
		"name": "Foo",
	})
	assert.NoError(t, err)
	d.MockedHTTPTransport().RegisterResponder("GET", `=~/storage/components/keboola.foo`, getGenericExResponder)

	// Add invalid objects
	branchKey := model.BranchKey{ID: 456}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(``, `branch`),
			},
		},
		Local: &model.Branch{
			BranchKey: branchKey,
		},
	}
	assert.NoError(t, state.Set(branchState))

	// Add invalid config
	configKey := model.ConfigKey{BranchID: 456, ComponentID: "keboola.foo", ID: "234"}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(`branch`, `config`),
			},
		},
		Remote: &model.Config{
			ConfigKey: configKey,
		},
	}
	assert.NoError(t, state.Set(configState))

	// Validate
	localErr, remoteErr := state.Validate(context.Background())
	expectedLocalError := `
local branch "branch" is not valid:
- "name" is a required field
`
	expectedRemoteError := `
remote config "branch:456/component:keboola.foo/config:234" is not valid:
- "name" is a required field
- "configuration" is a required field
`
	assert.Error(t, localErr)
	assert.Error(t, remoteErr)
	assert.Equal(t, strings.TrimSpace(expectedLocalError), localErr.Error())
	assert.Equal(t, strings.TrimSpace(expectedRemoteError), remoteErr.Error())
}
