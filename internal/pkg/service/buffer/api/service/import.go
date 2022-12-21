package service

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

func (s *service) Import(d dependencies.ForPublicRequest, payload *buffer.ImportPayload, reader io.ReadCloser) (err error) {
	ctx, str, header, ip, stats := d.RequestCtx(), d.Store(), d.RequestHeader(), d.RequestClientIP(), s.stats

	receiverKey := key.ReceiverKey{ProjectID: payload.ProjectID, ReceiverID: payload.ReceiverID}
	receiver, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return err
	}
	if receiver.Secret != payload.Secret {
		return &buffer.GenericError{
			StatusCode: 404,
			Name:       "buffer.receiverNotFound",
			Message:    fmt.Sprintf(`Receiver "%s" with given secret not found.`, payload.ReceiverID),
		}
	}

	data, err := parseRequestBody(payload.ContentType, reader)
	if err != nil {
		return err
	}

	importCtx := column.NewImportCtx(data, header, ip)
	receivedAt := time.Now()

	errs := errors.NewMultiError()
	for _, e := range receiver.Exports {
		// nolint:godox
		// TODO get sliceID and fileID + use in stats.Notify
		fileKey := key.FileKey{ExportKey: e.ExportKey, FileID: key.FileID(receivedAt)}
		sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(receivedAt)}

		csv := make([]string, 0)
		for _, c := range e.Mapping.Columns {
			csvValue, err := c.CsvValue(importCtx)
			if err != nil {
				return err
			}
			csv = append(csv, csvValue)
		}

		record := key.NewRecordKey(sliceKey, receivedAt)
		err = str.CreateRecord(ctx, record, csv)
		if err != nil {
			errs.AppendWithPrefixf(err, `failed to create record for export "%s"`, e.ExportID)
		}

		size := uint64(0)
		for _, column := range csv {
			size += uint64(len(column))
		}
		stats.Notify(sliceKey, size)
	}

	return errs.ErrorOrNil()
}

func parseRequestBody(contentType string, reader io.ReadCloser) (res *orderedmap.OrderedMap, err error) {
	if !isContentTypeForm(contentType) && !regexp.MustCompile(`^application/([a-zA-Z0-9\.\-]+\+)?json$`).MatchString(contentType) {
		return nil, NewUnsupportedMediaTypeError(errors.New(
			"Supported media types are application/json and application/x-www-form-urlencoded.",
		))
	}
	// Limit read csv to 1 MB plus 1 B. If the reader fills the limit then the request is bigger than allowed.
	limit := store.MaxImportRequestSizeInBytes
	limitedReader := io.LimitReader(reader, int64(limit)+1)
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = errors.Errorf("cannot close request body reading: %w", closeErr)
		}
	}()

	buf := new(strings.Builder)
	_, err = io.Copy(buf, limitedReader)
	if err != nil {
		return nil, err
	}

	// Check that the reader did not read more than the maximum.
	if datasize.ByteSize(buf.Len()) > limit {
		return nil, NewPayloadTooLargeError(errors.Errorf("Payload too large, the maximum size is %s.", limit.String()))
	}

	var data *orderedmap.OrderedMap
	if isContentTypeForm(contentType) {
		data, err = utilsUrl.ParseQuery(buf.String())
		if err != nil {
			return nil, NewBadRequestError(errors.New("Could not parse form request body."))
		}
	} else {
		err = json.Unmarshal([]byte(buf.String()), &data)
		if err != nil {
			return nil, NewBadRequestError(errors.New("Could not parse json request body."))
		}
	}
	if data.Len() == 0 {
		return nil, NewBadRequestError(errors.New("Empty request body."))
	}
	return data, nil
}

func isContentTypeForm(t string) bool {
	return strings.HasPrefix(t, "application/x-www-form-urlencoded")
}
