package remote

import (
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"

	"keboola-as-code/src/client"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
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
	response := a.CreatEventRequest(level, message, duration, params, results).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Event), nil
	}
	return nil, response.Err()
}

func (a *StorageApi) CreatEventRequest(
	level string,
	message string,
	duration time.Duration,
	params map[string]interface{},
	results map[string]interface{},
) *client.Request {
	paramsJson, err := json.EncodeString(params, false)
	if err != nil {
		panic(utils.PrefixError(`cannot serialize event "params" key to JSON`, err))
	}
	resultsJson, err := json.EncodeString(results, false)
	if err != nil {
		panic(utils.PrefixError(`cannot serialize event "results" key to JSON`, err))
	}

	return a.
		NewRequest(resty.MethodPost, "events").
		SetBody(map[string]string{
			"component": EventsComponentId,
			"message":   message,
			"type":      level,
			"duration":  fmt.Sprintf("%.0f", float64(duration/time.Second)),
			"params":    paramsJson,
			"results":   resultsJson,
		}).
		SetResult(&model.Event{})
}
