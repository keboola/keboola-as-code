package scheduler_test

import (
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSchedulerMapperRemoteDeactivate(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{
		ID: 123,
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
	require.NoError(t, state.Set(branchState))

	// Scheduler config
	schedulerKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: keboola.SchedulerComponentID,
		ID:          `456`,
	}
	schedulerConfigState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: schedulerKey,
		},
		Remote: &model.Config{
			ConfigKey: schedulerKey,
		},
	}
	require.NoError(t, state.Set(schedulerConfigState))

	// Expected HTTP call
	var httpRequest *http.Request
	d.MockedHTTPTransport().RegisterResponder("GET", `/v2/storage/?exclude=components`,
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewStringResponse(200, `{
				"services": [
					{
						"id": "scheduler",
						"url": "https://scheduler.connection.test"
					}
				],
				"features": []
			}`), nil
		},
	)
	d.MockedHTTPTransport().RegisterResponder(resty.MethodDelete, `=~configurations/456`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			return httpmock.NewStringResponse(200, `{"id": "789"}`), nil
		},
	)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddDeleted(schedulerConfigState)
	require.NoError(t, state.Mapper().AfterRemoteOperation(t.Context(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check API request
	assert.NotNil(t, httpRequest)
}
