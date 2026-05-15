package dummy

import (
	"context"
	"sync"
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
	// PipelineWriteHook overrides PipelineWriteError when non-nil. It is invoked
	// for each WriteRecord call so tests can produce per-record outcomes (e.g.
	// fail every other record to exercise partial_success paths).
	PipelineWriteHook func(sinkKey key.SinkKey) error
	UploadHandler     func(ctx context.Context, volume *diskreader.Volume, slice plugin.Slice, stats statistics.Value) error
	UploadError       error
	ImportHandler     func(ctx context.Context, file plugin.File, stats statistics.Value) error
	ImportError       error

	// counts tracks how many records were written to each sink so tests can
	// assert per-sink routing (which sinks were selected for a given record).
	countsMu sync.Mutex
	counts   map[key.SinkKey]int
}

// WriteCount returns the number of WriteRecord calls observed for the given sink.
func (c *SinkController) WriteCount(k key.SinkKey) int {
	c.countsMu.Lock()
	defer c.countsMu.Unlock()
	return c.counts[k]
}

// ResetWriteCounts clears all per-sink counters.
func (c *SinkController) ResetWriteCounts() {
	c.countsMu.Lock()
	defer c.countsMu.Unlock()
	c.counts = nil
}

func (c *SinkController) recordWrite(k key.SinkKey) {
	c.countsMu.Lock()
	defer c.countsMu.Unlock()
	if c.counts == nil {
		c.counts = make(map[key.SinkKey]int)
	}
	c.counts[k]++
}

type Pipeline struct {
	controller *SinkController
	sinkKey    key.SinkKey
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
			return controller.OpenPipeline(sinkKey, onClose)
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

func (c *SinkController) OpenPipeline(sinkKey key.SinkKey, onClose func(ctx context.Context, cause string)) (pipeline.Pipeline, error) {
	if c.PipelineOpenError != nil {
		return nil, c.PipelineOpenError
	}
	return &Pipeline{controller: c, sinkKey: sinkKey, onClose: onClose}, nil
}

func (p *Pipeline) ReopenOnSinkModification() bool {
	return p.controller.PipelineReopenOnSinkModification
}

func (p *Pipeline) WriteRecord(_ recordctx.Context) (pipeline.WriteResult, error) {
	p.controller.recordWrite(p.sinkKey)
	if hook := p.controller.PipelineWriteHook; hook != nil {
		if err := hook(p.sinkKey); err != nil {
			return pipeline.WriteResult{Status: pipeline.RecordError}, err
		}
		return pipeline.WriteResult{Status: p.controller.PipelineWriteRecordStatus}, nil
	}
	if err := p.controller.PipelineWriteError; err != nil {
		return pipeline.WriteResult{Status: pipeline.RecordError}, err
	}
	return pipeline.WriteResult{Status: p.controller.PipelineWriteRecordStatus}, nil
}

func (p *Pipeline) Close(ctx context.Context, cause string) {
	p.onClose(ctx, cause)
}
