package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Api struct {
	hostUrl string
	client  *client.Client
	logger  *zap.SugaredLogger
}

// Error represents Scheduler API error structure.
type Error struct {
	Message     string `json:"error"`
	ErrCode     int    `json:"code"`
	ExceptionId string `json:"exceptionId"`
}

func (e *Error) Error() string {
	msg := fmt.Sprintf(`"%v", errCode: "%v"`, e.Message, e.ErrCode)
	if len(e.ExceptionId) > 0 {
		msg += fmt.Sprintf(`, exceptionId: "%s"`, e.ExceptionId)
	}
	return msg
}

func NewSchedulerApi(ctx context.Context, logger *zap.SugaredLogger, hostUrl string, token string, verbose bool) *Api {
	c := client.NewClient(ctx, logger, verbose).WithHostUrl(hostUrl)
	c.SetHeader("X-StorageApi-Token", token)
	c.SetError(&Error{})
	return &Api{client: c, logger: logger, hostUrl: hostUrl}
}

func (a *Api) HttpClient() *http.Client {
	return a.client.GetRestyClient().GetClient()
}

func (a *Api) SetRetry(count int, waitTime time.Duration, maxWaitTime time.Duration) {
	a.client.SetRetry(count, waitTime, maxWaitTime)
}

func (a *Api) NewPool() *client.Pool {
	return a.client.NewPool(a.logger)
}

func (a *Api) OnObjectCreateUpdate(configKey model.ConfigKey, pool *client.Pool) {
	pool.Request(a.ActivateScheduleRequest(configKey.Id, "")).Send()
}

func (a *Api) OnObjectDelete(configKey model.ConfigKey, pool *client.Pool) {
	pool.Request(a.DeleteSchedulesForConfigurationRequest(configKey.Id)).Send()
}

// ActivateScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/activate
func (a *Api) ActivateScheduleRequest(configurationId string, configurationVersionId string) *client.Request {
	schedule := &model.Schedule{}
	body := map[string]string{
		"configurationId": configurationId,
	}
	if configurationVersionId != "" {
		body["configurationVersionId"] = configurationVersionId
	}
	return a.client.
		NewRequest(resty.MethodPost, "schedules").
		SetJsonBody(body).
		SetResult(schedule)
}

func (a *Api) ActivateSchedule(configurationId string, configurationVersionId string) (*model.Schedule, error) {
	response := a.ActivateScheduleRequest(configurationId, configurationVersionId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Schedule), nil
	}
	return nil, response.Err()
}

// DeleteScheduleRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedule
func (a *Api) DeleteScheduleRequest(scheduleId string) *client.Request {
	return a.client.
		NewRequest(resty.MethodDelete, "schedules/{scheduleId}").
		SetPathParam("scheduleId", scheduleId)
}

func (a *Api) DeleteSchedule(scheduleId string) error {
	return a.DeleteScheduleRequest(scheduleId).Send().Err()
}

// DeleteSchedulesForConfigurationRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/deleteSchedulesForConfiguration
func (a *Api) DeleteSchedulesForConfigurationRequest(configurationId string) *client.Request {
	return a.client.
		NewRequest(resty.MethodDelete, "configurations/{configurationId}").
		SetPathParam("configurationId", configurationId)
}

func (a *Api) DeleteSchedulesForConfiguration(configurationId string) error {
	return a.DeleteSchedulesForConfigurationRequest(configurationId).Send().Err()
}

// ListSchedulesRequest https://app.swaggerhub.com/apis/odinuv/scheduler/1.0.0#/schedules/get_schedules
func (a *Api) ListSchedulesRequest() *client.Request {
	schedules := make([]*model.Schedule, 0)
	return a.client.
		NewRequest(resty.MethodGet, "schedules").
		SetResult(&schedules)
}

func (a *Api) ListSchedules() ([]*model.Schedule, error) {
	response := a.ListSchedulesRequest().Send().Response
	if response.HasResult() {
		return *response.Result().(*[]*model.Schedule), nil
	}
	return nil, response.Err()
}
