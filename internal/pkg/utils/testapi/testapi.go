package testapi

import (
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

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
	Id   storageapi.ComponentID   `json:"id"`
	Type string                   `json:"type"`
	Name string                   `json:"name"`
	Data storageapi.ComponentData `json:"data"`
}

func mockedComponents() storageapi.Components {
	return storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: "foo.bar"}, Type: "other", Name: "Foo Bar", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "ex-generic-v2"}, Type: "extractor", Name: "Generic", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.foo.bar"}, Type: "other", Name: "Foo Bar", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.wr-db-mysql"}, Type: "writer", Name: "MySQL", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.ex-db-mysql"}, Type: "extractor", Name: "MySQL", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.ex-aws-s3"}, Type: "extractor", Name: "AWS S3", Data: storageapi.ComponentData{DefaultBucket: true, DefaultBucketStage: "in"}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.snowflake-transformation"}, Type: "transformation", Name: "Snowflake", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.python-transformation-v2"}, Type: "transformation", Name: "Python", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: storageapi.SharedCodeComponentID}, Type: "other", Name: "Shared Code", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: storageapi.VariablesComponentID}, Type: "other", Name: "Variables", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: storageapi.SchedulerComponentID}, Type: "other", Name: "Scheduler", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: storageapi.OrchestratorComponentID}, Type: "other", Name: "Orchestrator", Data: storageapi.ComponentData{}},
	}
}
