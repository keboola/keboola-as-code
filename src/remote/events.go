package remote

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
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
) (*model.Event, error) {
	response := a.CreatEventRequest(level, message, duration, params, results).Send().Response()
	if response.HasResult() {
		return response.Result().(*model.Event), nil
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
	paramsJson, err := json.Encode(params, false)
	if err != nil {
		panic(fmt.Errorf(`cannot serialize event "params" key to JSON: %s`, err))
	}
	resultsJson, err := json.Encode(results, false)
	if err != nil {
		panic(fmt.Errorf(`cannot serialize event "results" key to JSON: %s`, err))
	}

	return a.
		NewRequest(resty.MethodPost, "events").
		SetBody(map[string]string{
			"component": EventsComponentId,
			"message":   message,
			"type":      level,
			"duration":  fmt.Sprintf("%.0f", float64(duration/time.Second)),
			"params":    string(paramsJson),
			"results":   string(resultsJson),
		}).
		SetResult(&model.Event{})
}
