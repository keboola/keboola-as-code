// Package bridge provides the dispatch of events for platform telemetry purposes.
// Events contain slice/file statistics, for example, for billing purposes.
package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Schema: https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.keboola-buffer.json
const componentID = keboola.ComponentID("keboola.keboola-buffer")

type Params struct {
	ProjectID keboola.ProjectID
	SourceID  key.SourceID
	SinkID    key.SinkID
	Stats     statistics.Value
}

func (b *Bridge) SendSliceUploadEvent(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	duration time.Duration,
	errPtr *error,
	sliceKey model.SliceKey,
	stats statistics.Value,
) error {
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

	err = b.sendEvent(ctx, api, duration, "slice-upload", err, formatMsg, Params{
		ProjectID: sliceKey.ProjectID,
		SourceID:  sliceKey.SourceID,
		SinkID:    sliceKey.SinkID,
		Stats:     stats,
	})

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}

	return err
}

func (b *Bridge) SendFileImportEvent(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	duration time.Duration,
	errPtr *error,
	fileKey model.FileKey,
	stats statistics.Value,
) error {
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

	err = b.sendEvent(ctx, api, duration, "file-import", err, formatMsg, Params{
		ProjectID: fileKey.ProjectID,
		SourceID:  fileKey.SourceID,
		SinkID:    fileKey.SinkID,
		Stats:     stats,
	})

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}

	return err
}

func (b *Bridge) sendEvent(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	duration time.Duration,
	eventName string,
	err error,
	msg func(error) string,
	params Params,
) error {
	event := &keboola.Event{
		ComponentID: componentID,
		Message:     msg(err),
		Type:        "info",
		Duration:    client.DurationSeconds(duration),
		Params: map[string]any{
			"eventName": eventName,
			// legacy fields for compatibility with buffer events
			"task": eventName,
		},
		Results: map[string]any{
			"projectId": params.ProjectID,
			"sourceId":  params.SourceID,
			"sinkId":    params.SinkID,
			// legacy fields for compatibility with buffer events
			"receiverId": params.SourceID,
			"exportId":   params.SinkID,
		},
	}
	if err != nil {
		event.Type = "error"
		event.Results["error"] = fmt.Sprintf("%s", err)
	} else {
		event.Results["statistics"] = map[string]any{
			"firstRecordAt":    params.Stats.FirstRecordAt.String(),
			"lastRecordAt":     params.Stats.LastRecordAt.String(),
			"recordsCount":     params.Stats.RecordsCount,
			"slicesCount":      params.Stats.SlicesCount,
			"uncompressedSize": params.Stats.UncompressedSize.Bytes(),
			"compressedSize":   params.Stats.CompressedSize.Bytes(),
			"stagingSize":      params.Stats.StagingSize.Bytes(),
			// legacy fields for compatibility with buffer events
			"recordsSize":  params.Stats.CompressedSize.Bytes(),
			"bodySize":     params.Stats.UncompressedSize.Bytes(),
			"fileSize":     params.Stats.UncompressedSize.Bytes(),
			"fileGZipSize": params.Stats.CompressedSize.Bytes(),
		}
	}

	event, err = api.CreateEventRequest(event).Send(ctx)
	if err == nil {
		b.logger.Debugf(ctx, "Sent eventID: %v", event.ID)
	}

	return err
}
