package receive

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Importer imports records received by the import endpoint to etcd temporal database.
// Later, the records will be uploaded and imported to the database backend by a Worker.
type Importer struct {
	clock   clock.Clock
	store   *store.Store
	stats   *statistics.APINode
	watcher *watcher.APINode
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Store() *store.Store
	StatsAPINode() *statistics.APINode
	WatcherAPINode() *watcher.APINode
}

type requestDeps interface {
	RequestHeader() http.Header
	RequestClientIP() net.IP
}

func NewImporter(d dependencies) *Importer {
	return &Importer{
		clock:   d.Clock(),
		store:   d.Store(),
		stats:   d.StatsAPINode(),
		watcher: d.WatcherAPINode(),
	}
}

// CreateRecord in etcd temporal database.
func (r *Importer) CreateRecord(ctx context.Context, d requestDeps, receiverKey key.ReceiverKey, secret string, bodyReader io.ReadCloser) error {
	// Get cached receiver from the memory
	receiver, found, unlock := r.watcher.GetReceiver(receiverKey)
	if !found {
		return NewResourceNotFoundError("receiver", receiverKey.ReceiverID.String(), "project")
	}
	defer unlock()

	// Verify secret
	if receiver.Secret != secret {
		return &buffer.GenericError{
			StatusCode: 404,
			Name:       "buffer.receiverNotFound",
			Message:    fmt.Sprintf(`Receiver "%s" with given secret not found.`, receiverKey.ReceiverID),
		}
	}

	body, bodySize, err := ReadBody(bodyReader)
	if err != nil {
		return errors.Errorf(`cannot read request body: %w`, err)
	}

	now := r.clock.Now()
	receiveCtx := receivectx.New(ctx, now, d.RequestClientIP(), d.RequestHeader(), body)
	errs := errors.NewMultiErrorNoTrace()
	for _, slice := range receiver.Slices {
		// Format CSV row
		csvRow, err := FormatCSVRow(receiveCtx, slice.Mapping)
		if err != nil {
			// Wrap error with export ID
			err = errors.PrefixErrorf(err, `failed to format record for export "%s"`, slice.ExportID)

			// Convert FormatCSVRow error to the BadRequestError, if it doesn't have a specific HTTP code
			if HTTPCodeFrom(err) == http.StatusInternalServerError {
				err = NewBadRequestError(err)
			}

			errs.Append(err)
			continue
		}

		// Persist record
		recordKey := key.NewRecordKey(slice.SliceKey, now)
		if err := r.store.CreateRecord(ctx, recordKey, csvRow); err != nil {
			errs.AppendWithPrefixf(err, `failed to persist record for export "%s"`, slice.ExportID)
			continue
		}

		// Update statistics
		r.stats.Notify(slice.SliceKey, uint64(len(csvRow)), uint64(bodySize))
	}

	if errs.Len() > 1 {
		return WrapMultipleErrors(errs, http.StatusBadRequest)
	}
	return errs.ErrorOrNil()
}
