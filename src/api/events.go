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

func (a *StorageApi) CreateEvent(
	level string,
	message string,
	duration time.Duration,
	params map[string]interface{},
	results map[string]interface{},
) (*remote.Event, error) {
	response := a.CreatEventRequest(level, message, duration, params, results).Send().Response()
	if response.HasResult() {
		return response.Result().(*remote.Event), nil
	}
	return nil, response.Error()
}

func (a *StorageApi) CreatEventRequest(
	level string,
	message string,
	duration time.Duration,
	params map[string]interface{},
	results map[string]interface{},
) *client.Request {
	paramsJson, err := json.Marshal(params)
	if err != nil {
		panic(fmt.Errorf(`cannot serialize event "params" key to JSON: %s`, err))
	}
	resultsJson, err := json.Marshal(results)
	if err != nil {
		panic(fmt.Errorf(`cannot serialize event "results" key to JSON: %s`, err))
	}

	return a.
		Request(resty.MethodPost, "events").
		SetHeader("Content-Type", "application/x-www-form-urlencoded").
		SetMultipartFormData(map[string]string{
			"component": EventsComponentId,
			"message":   message,
			"type":      level,
			"duration":  fmt.Sprintf("%.0f", float64(duration/time.Second)),
			"params":    string(paramsJson),
			"results":   string(resultsJson),
		}).
		SetResult(remote.Event{})
}
