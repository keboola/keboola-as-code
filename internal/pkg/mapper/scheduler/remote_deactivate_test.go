package scheduler_test

import (
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
)

func TestSchedulerMapperRemoteDeactivate(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)
	schedulerApi, httpTransport := testapi.NewMockedSchedulerApi(log.NewDebugLogger())
	mapper := NewMapper(context, schedulerApi)

	// Branch
	branchKey := model.BranchKey{
		Id: 123,
	}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
		},
		Remote: &model.Branch{
			BranchKey: branchKey,
			IsDefault: true,
		},
	}
	assert.NoError(t, context.State.Set(branchState))

	// Scheduler config
	schedulerKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: model.SchedulerComponentId,
		Id:          `456`,
	}
	schedulerConfigState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: schedulerKey,
		},
		Remote: &model.Config{
			ConfigKey: schedulerKey,
		},
	}
	assert.NoError(t, context.State.Set(schedulerConfigState))

	// Expected HTTP call
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodDelete, `=~configurations/456`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			return httpmock.NewStringResponse(200, `{"id": "789"}`), nil
		},
	)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddDeleted(schedulerConfigState)
	assert.NoError(t, mapper.OnRemoteChange(changes))

	// Check API request
	assert.NotNil(t, httpRequest)
}
