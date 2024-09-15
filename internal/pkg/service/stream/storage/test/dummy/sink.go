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
	Provider                 = targetModel.Provider("test")
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
	UploadHandler                    func(ctx context.Context, volume *diskreader.Volume, slice plugin.Slice, stats statistics.Value) error
	UploadError                      error
	ImportHandler                    func(ctx context.Context, file plugin.File, stats statistics.Value) error
	ImportError                      error
}

type Pipeline struct {
	controller *SinkController
	onClose    func(ctx context.Context, cause string)
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
	plugins.RegisterSinkPipelineOpener(func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType, onClose func(ctx context.Context, cause string)) (pipeline.Pipeline, error) {
		if sinkType == SinkType {
			return controller.OpenPipeline(onClose)
		}

		return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
	})

	// Register dummy file importer
	plugins.RegisterFileImporter(
		Provider,
		func(ctx context.Context, file plugin.File, stats statistics.Value) error {
			if c.ImportHandler != nil {
				return c.ImportHandler(ctx, file, stats)
			}
			return c.ImportError
		},
	)
	// Register dummy sink with local storage support for tests
	plugins.RegisterSliceUploader(
		FileProvider,
		func(ctx context.Context, volume *diskreader.Volume, slice plugin.Slice, stats statistics.Value) error {
			if c.UploadHandler != nil {
				return c.UploadHandler(ctx, volume, slice, stats)
			}
			return c.UploadError
		},
	)
}

func (c *SinkController) OpenPipeline(onClose func(ctx context.Context, cause string)) (pipeline.Pipeline, error) {
	if c.PipelineOpenError != nil {
		return nil, c.PipelineOpenError
	}
	return &Pipeline{controller: c, onClose: onClose}, nil
}

func (p *Pipeline) ReopenOnSinkModification() bool {
	return p.controller.PipelineReopenOnSinkModification
}

func (p *Pipeline) WriteRecord(_ recordctx.Context) (pipeline.WriteResult, error) {
	if err := p.controller.PipelineWriteError; err != nil {
		return pipeline.WriteResult{Status: pipeline.RecordError}, err
	}
	return pipeline.WriteResult{Status: p.controller.PipelineWriteRecordStatus}, nil
}

func (p *Pipeline) Close(ctx context.Context, cause string) {
	p.onClose(ctx, cause)
}
