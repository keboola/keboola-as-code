package event

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const componentID = keboola.ComponentID("keboola.keboola-buffer")

type Sender struct {
	logger log.Logger
}

type dependencies interface {
	Logger() log.Logger
}

func NewSender(d dependencies) *Sender {
	return &Sender{logger: d.Logger()}
}

type Params struct {
	ProjectID  keboola.ProjectID
	ReceiverID key.ReceiverID
	ExportID   key.ExportID
	Stats      statistics.Value
}

func (s *Sender) SendSliceUploadEvent(ctx context.Context, api *keboola.API, start time.Time, errPtr *error, slice model.Slice, stats statistics.Value) {
	// Get error
	var err error
	if errPtr != nil {
		err = *errPtr
	}

	// Catch panic
	panicErr := recover()
	if panicErr != nil {
		err = errors.Errorf(`%s`, panicErr)
	}

	formatMsg := func(err error) string {
		if err != nil {
			return "Slice upload failed."
		} else {
			return "Slice upload done."
		}
	}

	s.sendEvent(ctx, api, start, err, "slice-upload", formatMsg, Params{
		ProjectID:  slice.ProjectID,
		ReceiverID: slice.ReceiverID,
		ExportID:   slice.ExportID,
		Stats:      stats,
	})

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}
}

func (s *Sender) SendFileImportEvent(ctx context.Context, api *keboola.API, start time.Time, errPtr *error, file model.File, stats statistics.Value) {
	// Get error
	var err error
	if errPtr != nil {
		err = *errPtr
	}

	// Catch panic
	panicErr := recover()
	if panicErr != nil {
		err = errors.Errorf(`%s`, panicErr)
	}

	formatMsg := func(err error) string {
		if err != nil {
			return "File import failed."
		} else {
			return "File import done."
		}
	}

	s.sendEvent(ctx, api, start, err, "file-import", formatMsg, Params{
		ProjectID:  file.ProjectID,
		ReceiverID: file.ReceiverID,
		ExportID:   file.ExportID,
		Stats:      stats,
	})

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}
}

/*
Ok:
{
	"componentId": "keboola.keboola-buffer",
	"type": "info",
	"message": "...",
	"duration": "...",
	"params": {
		"task": "..."
	},
	"results": {
		"projectId":  "...",
		"receiverId": "...",
		"exportId":   "...",
		"statistics": {
			"lastRecordAt": "...",
			"recordsCount": "...",
			"recordsSize":  "...",
			"bodySize":     "...",
			"fileSize":     "...",
			"fileGZipSize": "...",
		},
	}
}

Error:
{
	"componentId": "keboola.keboola-buffer",
	"type": "error",
	"message": "...",
	"duration": "...",
	"params": {
		"task": "..."
	},
	"results": {
		"projectId": "...",
		"receiverId": "...",
		"exportId":   "...",
		"error": "...",
	}
}
*/

func (s *Sender) sendEvent(ctx context.Context, api *keboola.API, start time.Time, err error, task string, msg func(error) string, params Params) {
	event := &keboola.Event{
		ComponentID: componentID,
		Message:     msg(err),
		Type:        "info",
		Duration:    client.DurationSeconds(time.Since(start)),
		Params: map[string]any{
			"task": task,
		},
		Results: map[string]any{
			"projectId":  params.ProjectID,
			"receiverId": params.ReceiverID,
			"exportId":   params.ExportID,
		},
	}
	if err != nil {
		event.Type = "error"
		event.Results["error"] = fmt.Sprintf("%s", err)
	} else {
		event.Results["statistics"] = map[string]any{
			"lastRecordAt": params.Stats.LastRecordAt.String(),
			"recordsCount": params.Stats.RecordsCount,
			"recordsSize":  params.Stats.RecordsSize.Bytes(),
			"bodySize":     params.Stats.BodySize.Bytes(),
			"fileSize":     params.Stats.FileSize.Bytes(),
			"fileGZipSize": params.Stats.FileGZipSize.Bytes(),
		}
	}

	event, err = api.CreateEventRequest(event).Send(ctx)
	if err == nil {
		s.logger.DebugfCtx(ctx, "Sent \"%s\" event id: \"%s\"", task, event.ID)
	} else {
		s.logger.WarnfCtx(ctx, "Cannot send \"%s\" event: %s", task, err)
	}
}
