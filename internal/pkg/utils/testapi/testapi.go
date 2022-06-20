package testapi

import (
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func MockedComponentsMap() *model.ComponentsMap {
	return model.NewComponentsMap(MockedComponents())
}

func MockedComponents() storageapi.Components {
	return storageapi.Components{
		{ComponentKey: storageapi.ComponentKey{ID: "foo.bar"}, Type: "other", Name: "Foo Bar", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "ex-generic-v2"}, Type: "extractor", Name: "Generic", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.foo.bar"}, Type: "other", Name: "Foo Bar", Data: storageapi.ComponentData{}},
		{ComponentKey: storageapi.ComponentKey{ID: "keboola.my-component"}, Type: "other", Name: "My Component", Data: storageapi.ComponentData{}},
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
