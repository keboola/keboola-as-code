package service

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *service) Import(d dependencies.ForPublicRequest, payload *buffer.ImportPayload, bodyReader io.ReadCloser) (err error) {
	ctx, str := d.RequestCtx(), d.Store()

	// Get receiver
	receiverKey := key.ReceiverKey{ProjectID: payload.ProjectID, ReceiverID: payload.ReceiverID}
	receiver, found := s.state.Receiver(receiverKey)
	if !found {
		return NewResourceNotFoundError("receiver", payload.ReceiverID.String())
	}

	// Verify secret
	if receiver.Secret != payload.Secret {
		return &buffer.GenericError{
			StatusCode: 404,
			Name:       "buffer.receiverNotFound",
			Message:    fmt.Sprintf(`Receiver "%s" with given secret not found.`, payload.ReceiverID),
		}
	}

	body, err := receive.ReadBody(bodyReader)
	if err != nil {
		return errors.Errorf(`cannot read request body: %w`, err)
	}

	receiveCtx := receivectx.New(ctx, d.Clock().Now(), d.RequestClientIP(), d.RequestHeader(), body)
	errs := errors.NewMultiErrorNoTrace()
	for _, export := range receiver.Exports {
		// Format CSV row
		csvRow, err := receive.FormatCSVRow(receiveCtx, export)
		if err != nil {
			// Wrap error with export ID
			err = errors.PrefixErrorf(err, `failed to format record for export "%s"`, export.ExportID)

			// Convert FormatCSVRow error to the BadRequestError, if it doesn't have a specific HTTP code
			if HTTPCodeFrom(err) == http.StatusInternalServerError {
				err = NewBadRequestError(err)
			}

			errs.Append(err)
			continue
		}

		// Persist record
		sliceKey := export.OpenedSlice.SliceKey
		recordKey := key.NewRecordKey(sliceKey, time.Now())
		if err := str.CreateRecord(ctx, recordKey, csvRow); err != nil {
			errs.AppendWithPrefixf(err, `failed to persist record for export "%s"`, export.ExportID)
			continue
		}

		// Update statistics
		s.stats.Notify(sliceKey, uint64(len(csvRow)))
	}

	if errs.Len() > 1 {
		return WrapMultipleErrors(errs, http.StatusBadRequest)
	}
	return errs.ErrorOrNil()
}
