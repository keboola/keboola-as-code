package testapi

import (
	"context"
	"os"
	"time"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
)

func NewMockedStorageApi() (*remote.StorageApi, *httpmock.MockTransport, log.DebugLogger) {
	logger := log.NewDebugLogger()

	// Set short retry delay in tests
	api := remote.NewStorageApi("connection.keboola.com", context.Background(), logger, false)
	api.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)
	api = api.WithToken(model.Token{Owner: model.TokenOwner{Id: 12345}})

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	api.HttpClient().Transport = transport
	return api, transport, logger
}

func NewMockedSchedulerApi() (*scheduler.Api, *httpmock.MockTransport, log.DebugLogger) {
	logger := log.NewDebugLogger()

	// Set short retry delay in tests
	api := scheduler.NewSchedulerApi(context.Background(), logger, "scheduler.keboola.com", "my-token", false)
	api.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	api.HttpClient().Transport = transport
	return api, transport, logger
}

func NewStorageApi(host string, verbose bool) (*remote.StorageApi, log.DebugLogger) {
	logger := log.NewDebugLogger()
	if verbose {
		logger.ConnectTo(os.Stdout)
	}
	a := remote.NewStorageApi(host, context.Background(), logger, false)
	a.SetRetry(3, 100*time.Millisecond, 100*time.Millisecond)
	return a, logger
}

func NewStorageApiWithToken(host, tokenStr string, verbose bool) (*remote.StorageApi, log.DebugLogger) {
	a, logger := NewStorageApi(host, verbose)
	token, err := a.GetToken(tokenStr)
	if err != nil {
		panic(err)
	}
	return a.WithToken(token), logger
}

func NewMockedComponentsProvider() model.RemoteComponentsProvider {
	api, httpTransport, _ := NewMockedStorageApi()
	AddMockedComponents(httpTransport)
	return api
}

func AddMockedComponents(httpTransport *httpmock.MockTransport) {
	// Register responses
	for _, component := range mockedComponents() {
		responder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
			"id": component.Id, "type": component.Type, "name": component.Name, "data": component.Data,
		})
		if err != nil {
			panic(err)
		}
		url := `=~/storage/components/` + component.Id.String()
		httpTransport.RegisterResponder("GET", url, responder)
	}
}

func AddMockedApiIndex(httpTransport *httpmock.MockTransport) {
	responder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"components": mockedComponents(),
	})
	if err != nil {
		panic(err)
	}
	url := `=~/storage$`
	httpTransport.RegisterResponder("GET", url, responder)
}

type MockedComponent struct {
	Id   model.ComponentId   `json:"id"`
	Type string              `json:"type"`
	Name string              `json:"name"`
	Data model.ComponentData `json:"data"`
}

func mockedComponents() []MockedComponent {
	return []MockedComponent{
		{"foo.bar", "other", "Foo Bar", model.ComponentData{}},
		{"ex-generic-v2", "extractor", "Generic", model.ComponentData{}},
		{"keboola.ex-db-mysql", "extractor", "MySQL", model.ComponentData{DefaultBucket: true, DefaultBucketStage: "in"}},
		{"keboola.snowflake-transformation", "transformation", "Snowflake", model.ComponentData{}},
		{"keboola.python-transformation-v2", "transformation", "Python", model.ComponentData{}},
		{model.SharedCodeComponentId, "other", "Shared Code", model.ComponentData{}},
		{model.VariablesComponentId, "other", "Variables", model.ComponentData{}},
		{model.SchedulerComponentId, "other", "Scheduler", model.ComponentData{}},
		{model.OrchestratorComponentId, "other", "Orchestrator", model.ComponentData{}},
	}
}
