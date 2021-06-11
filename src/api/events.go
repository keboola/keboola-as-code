package api

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
	"time"
)

const (
	EventsComponentId = "keboola.keboola-as-code"
)

func (a *StorageApi) SendEvent(level string, message string, duration time.Duration, data map[string]interface{}) (*remote.Event, error) {
	response, err := a.Send(a.SendEventReq(level, message, duration, data))
	if err == nil {
		return response.Result().(*remote.Event), nil
	}
	return nil, err
}

func (a *StorageApi) SendEventReq(level string, message string, duration time.Duration, data map[string]interface{}) *client.Request {
	dataJson, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Errorf("cannot serialize event data to JSON: %s", err))
	}

	return client.NewRequest(
		a.Req(resty.MethodPost, "events").
			SetHeader("Content-Type", "application/x-www-form-urlencoded").
			SetMultipartFormData(map[string]string{
				"component": EventsComponentId,
				"message":   message,
				"type":      level,
				"duration":  fmt.Sprintf("%.0f", float64(duration/time.Second)),
				"results":   string(dataJson),
			}).
			SetResult(remote.Event{}),
	)
}
