package state

import (
	"context"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestValidateState(t *testing.T) {
	t.Parallel()
	// Create state
	envs := env.Empty()
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", `123`)
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", `456`)

	logger, _ := utils.NewDebugLogger()
	m := loadTestManifest(t, envs, "minimal")
	m.Project.Id = 123

	api, httpTransport, _ := testapi.NewMockedStorageApi()

	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	stateOptions := NewOptions(m, api, schedulerApi, context.Background(), logger)
	s := newState(stateOptions)

	// Mocked component response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "keboola.foo",
		"type": "writer",
		"name": "Foo",
	})
	assert.NoError(t, err)
	httpTransport.RegisterResponder("GET", `=~/storage/components/keboola.foo`, getGenericExResponder)

	// Add invalid objects
	branchKey := model.BranchKey{Id: 456}
	branch := &model.Branch{BranchKey: branchKey}
	branchManifest := &model.BranchManifest{BranchKey: branchKey}
	branchManifest.ObjectPath = "branch"
	configKey := model.ConfigKey{BranchId: 456, ComponentId: "keboola.foo", Id: "234"}
	config := &model.Config{ConfigKey: configKey}
	configManifest := &model.ConfigManifest{ConfigKey: configKey}
	assert.NoError(t, s.manifest.PersistRecord(branchManifest))
	branchState, err := s.CreateFrom(branchManifest)
	assert.NoError(t, err)
	branchState.SetLocalState(branch)
	configState, err := s.CreateFrom(configManifest)
	assert.NoError(t, err)
	configState.SetRemoteState(config)
	assert.NoError(t, err)

	// Validate
	localErr, remoteErr := s.validate()
	expectedLocalError := `
local branch "branch" is not valid:
  - key="name", value="", failed "required" validation
`
	expectedRemoteError := `
remote config "branch:456/component:keboola.foo/config:234" is not valid:
  - key="name", value="", failed "required" validation
  - key="configuration", value="<nil>", failed "required" validation
`
	assert.Error(t, localErr)
	assert.Error(t, remoteErr)
	assert.Equal(t, strings.TrimSpace(expectedLocalError), localErr.Error())
	assert.Equal(t, strings.TrimSpace(expectedRemoteError), remoteErr.Error())
}
