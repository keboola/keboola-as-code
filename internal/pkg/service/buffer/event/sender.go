package event

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const componentID = keboola.ComponentID("keboola.keboola-buffer")

type Sender struct {
	logger log.Logger
	client *keboola.API
}

func NewSender(logger log.Logger, client *keboola.API) *Sender {
	return &Sender{logger: logger, client: client}
}

type EventParams struct {
	ProjectID  int
	ReceiverID string
	ExportID   string
	Stats      *model.Stats
}

func (s *Sender) SendSliceUploadEvent(ctx context.Context, start time.Time, err error, params EventParams) {
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

	s.sendEvent(ctx, start, err, "upload-slice", formatMsg, params)

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}
}

func (s *Sender) SendFileImportEvent(ctx context.Context, start time.Time, err error, params EventParams) {
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

	s.sendEvent(ctx, start, err, "file-import", formatMsg, params)

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

func (s *Sender) sendEvent(ctx context.Context, start time.Time, err error, task string, msg func(error) string, params EventParams) {
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
			"recordsSize":  params.Stats.RecordsSize,
			"bodySize":     params.Stats.BodySize,
			"fileSize":     params.Stats.FileSize,
			"fileGZipSize": params.Stats.FileGZipSize,
		}
	}

	event, err = s.client.CreateEventRequest(event).Send(ctx)
	if err == nil {
		s.logger.Debugf("Sent \"%s\" event id: \"%s\"", task, event.ID)
	} else {
		s.logger.Warnf("Cannot send \"%s\" event: %s", task, err)
	}
}
