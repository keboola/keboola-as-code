package testapi

import (
	"context"
	"os"
	"time"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func NewMockedStorageApi() (*remote.StorageApi, *httpmock.MockTransport, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()

	// Set short retry delay in tests
	api := remote.NewStorageApi("connection.keboola.com", context.Background(), logger, false)
	api.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)
	api = api.WithToken(model.Token{Owner: model.TokenOwner{Id: 12345}})

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	api.HttpClient().Transport = transport
	return api, transport, logs
}

func NewMockedSchedulerApi() (*scheduler.Api, *httpmock.MockTransport, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()

	// Set short retry delay in tests
	api := scheduler.NewSchedulerApi(context.Background(), logger, "scheduler.keboola.com", "my-token", false)
	api.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	api.HttpClient().Transport = transport
	return api, transport, logs
}

func NewStorageApi(host string, verbose bool) (*remote.StorageApi, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()
	if verbose {
		logs.ConnectTo(os.Stdout)
	}
	a := remote.NewStorageApi(host, context.Background(), logger, false)
	a.SetRetry(3, 100*time.Millisecond, 100*time.Millisecond)
	return a, logs
}

func NewStorageApiWithToken(host, tokenStr string, verbose bool) (*remote.StorageApi, *utils.Writer) {
	a, logs := NewStorageApi(host, verbose)
	token, err := a.GetToken(tokenStr)
	if err != nil {
		panic(err)
	}
	return a.WithToken(token), logs
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
			"id": component.Id, "type": component.Type, "name": component.Name,
		})
		if err != nil {
			panic(err)
		}
		url := `=~/storage/components/` + component.Id
		httpTransport.RegisterResponder("GET", url, responder)
	}
}

func AddMockedApiIndex(httpTransport *httpmock.MockTransport) {
	var components []interface{}
	for _, component := range mockedComponents() {
		components = append(components, map[string]interface{}{
			"id": component.Id, "type": component.Type, "name": component.Name,
		})
	}
	responder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"components": components,
	})
	if err != nil {
		panic(err)
	}
	url := `=~/storage$`
	httpTransport.RegisterResponder("GET", url, responder)
}

func mockedComponents() []struct{ Id, Type, Name string } {
	return []struct{ Id, Type, Name string }{
		{"foo.bar", "other", "Foo Bar"},
		{"ex-generic-v2", "extractor", "Generic"},
		{"keboola.ex-db-mysql", "extractor", "MySQL"},
		{"keboola.snowflake-transformation", "transformation", "Snowflake"},
		{"keboola.python-transformation-v2", "transformation", "Python"},
		{model.SharedCodeComponentId, "other", "Shared Code"},
		{model.VariablesComponentId, "other", "Variables"},
		{model.SchedulerComponentId, "other", "Scheduler"},
		{model.OrchestratorComponentId, "other", "Orchestrator"},
	}
}
