package receive

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/quota"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
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
	config         config.APIConfig
	clock          clock.Clock
	store          *store.Store
	watcher        *watcher.APINode
	statsCollector *statistics.CollectorNode
	statsCache     *statistics.CacheNode
	quota          *quota.Quota
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	APIConfig() config.APIConfig
	Store() *store.Store
	StatsCollector() *statistics.CollectorNode
	StatsCache() *statistics.CacheNode
	WatcherAPINode() *watcher.APINode
}

type requestDeps interface {
	RequestHeader() http.Header
	RequestClientIP() net.IP
}

func NewImporter(d dependencies) *Importer {
	i := &Importer{
		config:         d.APIConfig(),
		clock:          d.Clock(),
		store:          d.Store(),
		watcher:        d.WatcherAPINode(),
		statsCollector: d.StatsCollector(),
		statsCache:     d.StatsCache(),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		cancel()
		wg.Wait()
	})

	// Create quota checker
	i.quota = quota.New(ctx, wg, d)

	return i
}

// CreateRecord in etcd temporal database.
func (i *Importer) CreateRecord(ctx context.Context, d requestDeps, receiverKey key.ReceiverKey, secret string, bodyReader io.ReadCloser) error {
	// Get cached receiver from the memory
	receiver, found, unlock := i.watcher.GetReceiver(receiverKey)
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

	// Check whether the size of records that one receiver can buffer in etcd has not been exceeded.
	if err := i.quota.Check(receiverKey); err != nil {
		return NewInsufficientStorageError(err)
	}

	//  ReadBody, its length is limited.
	body, bodySize, err := ReadBody(bodyReader)
	if err != nil {
		return errors.Errorf(`cannot read request body: %w`, err)
	}

	now := i.clock.Now()
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
		if err := i.store.CreateRecord(ctx, recordKey, csvRow); err != nil {
			errs.AppendWithPrefixf(err, `failed to persist record for export "%s"`, slice.ExportID)
			continue
		}

		// Update statistics
		i.statsCollector.Notify(slice.SliceKey, datasize.ByteSize(len(csvRow)), datasize.ByteSize(bodySize))
	}

	if errs.Len() > 1 {
		return WrapMultipleErrors(errs, http.StatusBadRequest)
	}
	return errs.ErrorOrNil()
}
