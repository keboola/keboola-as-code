package dummy

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

const (
	FileProvider             = stagingModel.FileProvider("test")
	SinkType                 = definition.SinkType("test")
	SinkTypeWithLocalStorage = definition.SinkType("testWithLocalStorage")
)

type SinkController struct {
	FileMapping                      table.Mapping
	FileExpiration                   time.Duration
	PipelineReopenOnSinkModification bool
	PipelineOpenError                error
	PipelineWriteRecordStatus        pipeline.RecordStatus
	PipelineWriteError               error
	PipelineCloseError               error
	ImportError                      error
	UploadError                      error
}

type Pipeline struct {
	controller *SinkController
}

func NewController() *SinkController {
	return &SinkController{
		FileExpiration:                   24 * time.Hour,
		PipelineReopenOnSinkModification: true,
		PipelineWriteRecordStatus:        pipeline.RecordAccepted,
	}
}

func NewSink(k key.SinkKey) definition.Sink {
	return definition.Sink{
		SinkKey:     k,
		Type:        SinkType,
		Name:        "My Sink",
		Description: "My Description",
	}
}

func NewSinkWithLocalStorage(k key.SinkKey) definition.Sink {
	return definition.Sink{
		SinkKey:     k,
		Type:        SinkTypeWithLocalStorage,
		Name:        "My Sink",
		Description: "My Description",
	}
}

func (c *SinkController) RegisterDummySinkTypes(plugins *plugin.Plugins, controller *SinkController) {
	// Register dummy sink with local storage support for tests
	plugins.RegisterSinkWithLocalStorage(func(sinkType definition.SinkType) bool {
		return sinkType == SinkTypeWithLocalStorage
	})
	plugins.Collection().OnFileOpen(func(ctx context.Context, now time.Time, sink definition.Sink, file *model.File) error {
		if sink.Type == SinkTypeWithLocalStorage {
			// Set required fields
			file.Mapping = controller.FileMapping
			file.StagingStorage.Provider = "test"
			file.StagingStorage.Expiration = file.OpenedAt().Add(c.FileExpiration)
			file.TargetStorage.Provider = "test"
		}
		return nil
	})

	// Register dummy pipeline opener for tests
	plugins.RegisterSinkPipelineOpener(func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType) (pipeline.Pipeline, error) {
		if sinkType == SinkType {
			return controller.OpenPipeline()
		}

		return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
	})

	// Register dummy file importer
	plugins.RegisterFileImporter(targetModel.Provider("test"), func(ctx context.Context, file *plugin.File) error {
		return c.ImportError
	})
	// Register dummy sink with local storage support for tests
	plugins.RegisterSliceUploader(
		FileProvider,
		func(ctx context.Context, volume *diskreader.Volume, slice *plugin.Slice, stats statistics.Value) error {
			return c.UploadError
		})
}

func (c *SinkController) OpenPipeline() (pipeline.Pipeline, error) {
	if c.PipelineOpenError != nil {
		return nil, c.PipelineOpenError
	}
	return &Pipeline{controller: c}, nil
}

func (p *Pipeline) ReopenOnSinkModification() bool {
	return p.controller.PipelineReopenOnSinkModification
}

func (p *Pipeline) WriteRecord(_ recordctx.Context) (pipeline.RecordStatus, error) {
	if err := p.controller.PipelineWriteError; err != nil {
		return pipeline.RecordError, err
	}
	return p.controller.PipelineWriteRecordStatus, nil
}

func (p *Pipeline) Close(_ context.Context) error {
	return p.controller.PipelineCloseError
}
