package scheduler_test

import (
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSchedulerRemoteMapper_Deactivate(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
	_, httpTransport := d.UseMockedSchedulerApi()
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{Id: 123}
	branch := &model.Branch{
		BranchKey: branchKey,
		IsDefault: true,
	}
	state.MustAdd(branch)

	// Scheduler config
	schedulerKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.SchedulerComponentId,
		Id:          `456`,
	}
	schedulerConfig := &model.Config{
		ConfigKey: schedulerKey,
	}
	state.MustAdd(schedulerConfig)

	// Expected HTTP call
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodDelete, `=~configurations/456`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			return httpmock.NewStringResponse(200, `{"id": "789"}`), nil
		},
	)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddDeleted(schedulerConfig.Key())))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check API request
	assert.NotNil(t, httpRequest)
}
