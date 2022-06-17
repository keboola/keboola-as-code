package testapi

import (
	"github.com/jarcoal/httpmock"

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
