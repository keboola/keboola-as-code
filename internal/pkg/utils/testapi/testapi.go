package testapi

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func MockedComponentsMap() *model.ComponentsMap {
	return model.NewComponentsMap(MockedComponents())
}

func MockedComponents() keboola.Components {
	return keboola.Components{
		{ComponentKey: keboola.ComponentKey{ID: "foo.bar"}, Type: "other", Name: "Foo Bar", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: "ex-generic-v2"}, Type: "extractor", Name: "Generic", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: "keboola.foo.bar"}, Type: "other", Name: "Foo Bar", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: "keboola.my-component"}, Type: "other", Name: "My Component", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: "keboola.wr-db-mysql"}, Type: "writer", Name: "MySQL", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: "keboola.ex-db-mysql"}, Type: "extractor", Name: "MySQL", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: "keboola.ex-aws-s3"}, Type: "extractor", Name: "AWS S3", Data: keboola.ComponentData{DefaultBucket: true, DefaultBucketStage: "in"}},
		{ComponentKey: keboola.ComponentKey{ID: "keboola.snowflake-transformation"}, Type: "transformation", Name: "Snowflake", Data: keboola.ComponentData{}, Flags: []string{"genericCodeBlocksUI"}},
		{ComponentKey: keboola.ComponentKey{ID: "keboola.python-transformation-v2"}, Type: "transformation", Name: "Python", Data: keboola.ComponentData{}, Flags: []string{"genericCodeBlocksUI"}},
		{ComponentKey: keboola.ComponentKey{ID: keboola.SharedCodeComponentID}, Type: "other", Name: "Shared Code", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: keboola.VariablesComponentID}, Type: "other", Name: "Variables", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: keboola.SchedulerComponentID}, Type: "other", Name: "Scheduler", Data: keboola.ComponentData{}},
		{ComponentKey: keboola.ComponentKey{ID: keboola.OrchestratorComponentID}, Type: "other", Name: "Orchestrator", Data: keboola.ComponentData{}},
	}
}
