// Package bridge provides the dispatch of events for platform telemetry purposes.
// Events contain slice/file statistics, for example, for billing purposes.
package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Schema: https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.stream.sourceCreate.json
// Schema: https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.stream.sourceDelete.json
// Schema: https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.stream.sourcePurge.json
// Schema: https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.stream.sliceUpload.json
// Schema: https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.stream.fileImport.json
const (
	componentSourceCreateID = keboola.ComponentID("keboola.stream.sourceCreate")
	componentSourceDeleteID = keboola.ComponentID("keboola.stream.sourceDelete")
	componentSourcePurgeID  = keboola.ComponentID("keboola.stream.sourcePurge")
	componentSliceUploadID  = keboola.ComponentID("keboola.stream.sliceUpload")
	componentFileImportID   = keboola.ComponentID("keboola.stream.fileImport")
)

type Params struct {
	ProjectID  keboola.ProjectID
	SourceID   key.SourceID
	SourceName string
	SinkID     key.SinkID
	Stats      statistics.Value
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

	err = SendEvent(
		ctx,
		b.logger,
		api,
		componentSliceUploadID,
		duration,
		err,
		formatMsg,
		Params{
			ProjectID: sliceKey.ProjectID,
			SourceID:  sliceKey.SourceID,
			SinkID:    sliceKey.SinkID,
			Stats:     stats,
		},
	)

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

	err = SendEvent(
		ctx,
		b.logger,
		api,
		componentFileImportID,
		duration,
		err,
		formatMsg,
		Params{
			ProjectID: fileKey.ProjectID,
			SourceID:  fileKey.SourceID,
			SinkID:    fileKey.SinkID,
			Stats:     stats,
		},
	)

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}

	return err
}

func SendEvent(
	ctx context.Context,
	logger log.Logger,
	api *keboola.AuthorizedAPI,
	componentID keboola.ComponentID,
	duration time.Duration,
	err error,
	msg func(error) string,
	params Params,
) error {
	event := &keboola.Event{
		ComponentID: componentID,
		Message:     msg(err),
		Type:        "info",
		Duration:    client.DurationSeconds(duration),
		Results: map[string]any{
			"projectId": params.ProjectID,
			"sourceId":  params.SourceID,
			"sinkId":    params.SinkID,
		},
	}
	if params.SourceName != "" {
		event.Results["sourceName"] = params.SourceName
	}

	var sErr error
	defer func() {
		event, sErr = api.CreateEventRequest(event).Send(ctx)
		if sErr == nil {
			logger.Debugf(ctx, "Sent eventID: %v", event.ID)
		}
	}()

	if err != nil {
		event.Type = "error"
		event.Results["error"] = fmt.Sprintf("%s", err)
		return sErr
	}

	if params.Stats.RecordsCount > 0 {
		event.Results["statistics"] = map[string]any{
			"firstRecordAt":    params.Stats.FirstRecordAt.String(),
			"lastRecordAt":     params.Stats.LastRecordAt.String(),
			"recordsCount":     params.Stats.RecordsCount,
			"slicesCount":      params.Stats.SlicesCount,
			"uncompressedSize": params.Stats.UncompressedSize.Bytes(),
			"compressedSize":   params.Stats.CompressedSize.Bytes(),
			"stagingSize":      params.Stats.StagingSize.Bytes(),
		}
	}

	return sErr
}
