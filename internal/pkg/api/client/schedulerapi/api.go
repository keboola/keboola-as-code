// Package schedulerapi contains request definitions for the Scheduler API.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client.
// It is necessary to set the base URL and "X-StorageApi-Token" header in the HTTP client.
package schedulerapi

import (
	"net/http"

	. "github.com/keboola/keboola-as-code/internal/pkg/api/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// ActivateScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/activate
func ActivateScheduleRequest(configId model.ConfigId, configurationVersionId string) Request[*model.Schedule] {
	body := map[string]string{
		"configurationId": configId.String(),
	}
	if configurationVersionId != "" {
		body["configurationVersionId"] = configurationVersionId
	}
	return newRequest(&model.Schedule{}).
		SetMethod(http.MethodPost).
		SetUrl("schedules").
		SetJsonBody(body)
}

// DeleteScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedule
func DeleteScheduleRequest(scheduleId string) Request[NoResult] {
	return newRequest(NoResult{}).
		SetMethod(http.MethodDelete).
		SetUrl("schedules/{scheduleId}").
		SetPathParam("scheduleId", scheduleId)
}

// DeleteSchedulesForConfigurationRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedulesForConfiguration
func DeleteSchedulesForConfigurationRequest(configId model.ConfigId) Request[NoResult] {
	return newRequest(NoResult{}).
		SetMethod(http.MethodDelete).
		SetUrl("configurations/{configurationId}").
		SetPathParam("configurationId", configId.String())
}

// ListSchedulesRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
func ListSchedulesRequest() Request[[]*model.Schedule] {
	return newRequest(make([]*model.Schedule, 0)).
		SetMethod(http.MethodGet).
		SetUrl("schedules")
}

func newRequest[R Result](resultDef R) Request[R] {
	// Create request and set default error type
	return NewRequest(resultDef).SetErrorDef(&Error{})
}
