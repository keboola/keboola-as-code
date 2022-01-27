package storageapi

import (
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	EventsComponentId = "keboola.keboola-as-code"
)

func (a *Api) CreateEvent(
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

func (a *Api) CreatEventRequest(
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
		SetFormBody(map[string]string{
			"component": EventsComponentId,
			"message":   message,
			"type":      level,
			"duration":  fmt.Sprintf("%.0f", float64(duration/time.Second)),
			"params":    paramsJson,
			"results":   resultsJson,
		}).
		SetResult(&model.Event{})
}
