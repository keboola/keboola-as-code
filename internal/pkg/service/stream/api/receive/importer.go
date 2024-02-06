package receive

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/quota"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/watcher"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Importer imports records received by the import endpoint to etcd temporal database.
// Later, the records will be uploaded and imported to the database backend by a Worker.
type Importer struct {
	config         config.APIConfig
	clock          clock.Clock
	store          *store.Store
	watcher        *watcher.APINode
	statsCollector *statistics.Collector
	quota          *quota.Checker
	metrics        metrics
}

type metrics struct {
	Count    metric.Int64Counter
	BodySize metric.Int64Counter
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	APIConfig() config.APIConfig
	Store() *store.Store
	StatsCollector() *statistics.Collector
	StatisticsRepository() *statistics.Repository
	StatisticsL2Cache() *statistics.L2CacheProvider
	WatcherAPINode() *watcher.APINode
}

type requestDeps interface {
	RequestHeader() http.Header
	RequestClientIP() net.IP
}

func NewImporter(d dependencies) *Importer {
	return &Importer{
		config:         d.APIConfig(),
		clock:          d.Clock(),
		store:          d.Store(),
		watcher:        d.WatcherAPINode(),
		statsCollector: d.StatsCollector(),
		quota:          quota.New(d),
		metrics: metrics{
			Count:    d.Telemetry().Meter().Counter("keboola.go.buffer.ingress_records", "Count of received records.", ""),
			BodySize: d.Telemetry().Meter().Counter("keboola.go.buffer.ingress_bytes", "Sum of all request body bytes.", "bytes"),
		},
	}
}

// CreateRecord in etcd temporal database.
func (i *Importer) CreateRecord(ctx context.Context, d requestDeps, receiverKey key.ReceiverKey, secret string, bodyReader io.ReadCloser) (err error) {
	// Metrics
	var bodySize int64
	defer func() {
		attrs := []attribute.KeyValue{
			attribute.String("projectId", receiverKey.ProjectID.String()),
			attribute.String("receiverId", receiverKey.ReceiverID.String()),
			attribute.String("source", "http"),
			attribute.Bool("is_success", err == nil),
		}
		if err != nil {
			attrs = append(attrs,
				attribute.Bool("is_application_error", HTTPCodeFrom(err) >= http.StatusInternalServerError),
				attribute.String("error_type", telemetry.ErrorType(err)),
			)
		}
		attrsOption := metric.WithAttributes(attrs...)
		i.metrics.Count.Add(ctx, 1, attrsOption)
		i.metrics.BodySize.Add(ctx, bodySize, attrsOption)
	}()

	// Get cached receiver from the memory
	receiver, found, unlock := i.watcher.GetReceiver(receiverKey)
	if !found {
		return NewResourceNotFoundError("receiver", receiverKey.ReceiverID.String(), "project")
	}
	defer unlock()

	// Verify secret
	if receiver.Secret != secret {
		return &stream.GenericError{
			StatusCode: 404,
			Name:       "buffer.receiverNotFound",
			Message:    fmt.Sprintf(`Receiver "%s" with given secret not found.`, receiverKey.ReceiverID),
		}
	}

	// Check whether the size of records that one receiver can buffer in etcd has not been exceeded.
	if err := i.quota.Check(ctx, receiverKey); err != nil {
		return err
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
		i.statsCollector.Notify(i.clock.Now(), slice.SliceKey, datasize.ByteSize(len(csvRow)), datasize.ByteSize(bodySize))
	}

	if errs.Len() > 1 {
		return WrapMultipleErrors(errs, http.StatusBadRequest)
	}
	return errs.ErrorOrNil()
}
