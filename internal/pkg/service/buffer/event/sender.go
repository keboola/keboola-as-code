package event

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const componentID = storageapi.ComponentID("keboola.keboola-buffer")

type Sender struct {
	logger log.Logger
	client client.Sender
}

func NewSender(logger log.Logger, client client.Sender) *Sender {
	return &Sender{logger: logger, client: client}
}

func (s *Sender) SendSliceUploadEvent(ctx context.Context, start time.Time, err error, projectID int) {
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

	s.sendEvent(ctx, start, err, "upload-slice", formatMsg, projectID)

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}
}

func (s *Sender) SendFileImportEvent(ctx context.Context, start time.Time, err error, projectID int) {
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

	s.sendEvent(ctx, start, err, "file-import", formatMsg, projectID)

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
		"projectId": "...",
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
		"error": "...",
	}
}
*/

func (s *Sender) sendEvent(ctx context.Context, start time.Time, err error, task string, msg func(error) string, projectID int) {
	event := &storageapi.Event{
		ComponentID: componentID,
		Message:     msg(err),
		Type:        "info",
		Duration:    client.DurationSeconds(time.Since(start)),
		Params: map[string]interface{}{
			"task": task,
		},
		Results: map[string]interface{}{
			"projectId": projectID,
		},
	}
	if err != nil {
		event.Type = "error"
		event.Results["error"] = fmt.Sprintf("%s", err)
	}

	event, err = storageapi.CreatEventRequest(event).Send(ctx, s.client)
	if err == nil {
		s.logger.Debugf("Sent \"%s\" event id: \"%s\"", task, event.ID)
	} else {
		s.logger.Warnf("Cannot send \"%s\" event: %s", task, err)
	}
}
