// Package writernode provides entrypoint for the storage writer node.
// The node receives a stream of slice bytes over the network and stores them on the local disk.
package writernode

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode/diskcleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func Start(ctx context.Context, d dependencies.StorageWriterScope, cfg config.Config) error {
	logger := d.Logger().WithComponent("storage.node.writer")
	logger.Info(ctx, `starting storage writer node`)

	meter := d.Telemetry().Meter()

	meter.IntObservableGauge(
		"keboola.go.stream.writer.disk.used",
		"Amount of disk space used.",
		"B",
		func(ctx context.Context, observer metric.Int64Observer) error {
			errs := errors.NewMultiError()

			for _, volume := range d.Volumes().Collection().All() {
				space, err := volume.UsedSpace()
				if err != nil {
					errs.Append(err)
					continue
				}

				observer.Observe(int64(space), metric.WithAttributes(volume.Telemetry()...))
			}

			return errs.ErrorOrNil()
		},
	)

	meter.IntObservableGauge(
		"keboola.go.stream.writer.disk.total",
		"Total disk size.",
		"B",
		func(ctx context.Context, observer metric.Int64Observer) error {
			errs := errors.NewMultiError()

			for _, volume := range d.Volumes().Collection().All() {
				space, err := volume.TotalSpace()
				if err != nil {
					errs.Append(err)
					continue
				}

				observer.Observe(int64(space), metric.WithAttributes(volume.Telemetry()...))
			}

			return errs.ErrorOrNil()
		},
	)

	meter.IntObservableGauge(
		"keboola.go.stream.writer.file.descriptor.used",
		"Amount of file descriptors currently used.",
		"fd",
		func(ctx context.Context, observer metric.Int64Observer) error {
			used, err := telemetry.UsedFileDescriptors()
			if err == nil {
				observer.Observe(int64(used))
			}
			return err
		},
	)

	meter.IntObservableGauge(
		"keboola.go.stream.writer.file.descriptor.total",
		"Limit of available file descriptors.",
		"fd",
		func(ctx context.Context, observer metric.Int64Observer) error {
			limit, err := telemetry.TotalFileDescriptors()
			if err == nil {
				observer.Observe(int64(limit))
			}
			return err
		},
	)

	if err := rpc.StartNetworkFileServer(d, cfg.NodeID, cfg.Hostname, cfg.Storage.Level.Local); err != nil {
		return err
	}

	if err := diskcleanup.Start(d, cfg.Storage.DiskCleanup); err != nil {
		return err
	}

	return nil
}
