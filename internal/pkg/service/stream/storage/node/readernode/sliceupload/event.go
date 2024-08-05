package sliceupload

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

const componentID = keboola.ComponentID("keboola.keboola-as-code")

type Params struct {
	ProjectID keboola.ProjectID
	SourceID  key.SourceID
	SinkID    key.SinkID
	Stats     statistics.Value
}

func sendEvent(
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
	return err
}
