package dummy

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

const (
	SinkType                 = definition.SinkType("test")
	SinkTypeWithLocalStorage = definition.SinkType("testWithLocalStorage")
)

func RegisterDummySinkTypes(plugins *plugin.Plugins, testPipelineOpener *pipeline.TestOpener) {
	// Register dummy sink with local storage support for tests
	plugins.RegisterSinkWithLocalStorage(func(sinkType definition.SinkType) bool {
		return sinkType == SinkTypeWithLocalStorage
	})
	plugins.Collection().OnFileOpen(func(ctx context.Context, now time.Time, sink definition.Sink, file *model.File) error {
		if sink.Type == SinkTypeWithLocalStorage {
			// Set required fields
			file.Mapping = table.Mapping{Columns: column.Columns{column.Body{Name: "body"}}}
			file.StagingStorage.Provider = "test"
			file.TargetStorage.Provider = "test"
		}
		return nil
	})

	// Register dummy pipeline opener for tests
	plugins.RegisterSinkPipelineOpener(func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType) (pipeline.Pipeline, error) {
		if sinkType == SinkType {
			return testPipelineOpener.OpenPipeline()
		}

		return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
	})
}
