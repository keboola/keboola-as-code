package testapi

import (
	"context"
	"os"
	"time"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func NewMockedStorageApi(logger log.DebugLogger) (*storageapi.Api, *httpmock.MockTransport) {
	// Set short retry delay in tests
	api := storageapi.New(context.Background(), logger, "connection.keboola.com", false)
	api.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)
	api = api.WithToken(model.Token{Owner: model.TokenOwner{Id: 12345}})

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	api.HttpClient().Transport = transport
	return api, transport
}

func NewMockedSchedulerApi(logger log.DebugLogger) (*schedulerapi.Api, *httpmock.MockTransport) {
	// Set short retry delay in tests
	api := schedulerapi.New(context.Background(), logger, "scheduler.keboola.com", "my-token", false)
	api.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	api.HttpClient().Transport = transport
	return api, transport
}

func NewStorageApi(host string, verbose bool) (*storageapi.Api, log.DebugLogger) {
	logger := log.NewDebugLogger()
	if verbose {
		logger.ConnectTo(os.Stdout)
	}
	a := storageapi.New(context.Background(), logger, host, false)
	a.SetRetry(3, 100*time.Millisecond, 100*time.Millisecond)
	return a, logger
}

func NewStorageApiWithToken(host, tokenStr string, verbose bool) (*storageapi.Api, log.DebugLogger) {
	a, logger := NewStorageApi(host, verbose)
	token, err := a.GetToken(tokenStr)
	if err != nil {
		panic(err)
	}
	return a.WithToken(token), logger
}

func NewMockedComponentsProvider() model.RemoteComponentsProvider {
	api, httpTransport := NewMockedStorageApi(log.NewDebugLogger())
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
		{"keboola.foo.bar", "other", "Foo Bar", model.ComponentData{}},
		{"keboola.wr-db-mysql", "writer", "MySQL", model.ComponentData{}},
		{"keboola.ex-db-mysql", "extractor", "MySQL", model.ComponentData{}},
		{"keboola.ex-aws-s3", "extractor", "AWS S3", model.ComponentData{DefaultBucket: true, DefaultBucketStage: "in"}},
		{"keboola.snowflake-transformation", "transformation", "Snowflake", model.ComponentData{}},
		{"keboola.python-transformation-v2", "transformation", "Python", model.ComponentData{}},
		{model.SharedCodeComponentId, "other", "Shared Code", model.ComponentData{}},
		{model.VariablesComponentId, "other", "Variables", model.ComponentData{}},
		{model.SchedulerComponentId, "other", "Scheduler", model.ComponentData{}},
		{model.OrchestratorComponentId, "other", "Orchestrator", model.ComponentData{}},
	}
}
