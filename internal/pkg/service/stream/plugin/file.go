package plugin

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Plugins) RegisterFileImporter(sliceKey model.SliceKey) error {
	return nil
}

func (p *Plugins) ImportFile(ctx context.Context, slice *model.Slice, sinkSchema schema.Schema, client etcd.KV, volume *diskreader.Volume) error {
	var err error
	reader, err := volume.OpenReader(slice)
	if err != nil {
		// p.logger.Warnf(ctx, "unable to open reader: %v", err)
		return err
	}

	// credentials := sinkSchema.UploadCredentials().ForFile(slice.FileKey).GetOrEmpty(client).Do(ctx).Result()
	// token := sinkSchema.Token().ForSink(slice.SinkKey).GetOrEmpty(client).Do(ctx).Result()

	defer func() {
		err = reader.Close(ctx)
	}()

	/*uploader, err := p.sliceUploader[slice.SliceKey](ctx, &credentials.FileUploadCredentials, slice.String())
	if err != nil {
		return err
	}*/

	// Compress to GZip and measure count/size
	/*gzipWr, err := gzip.NewWriterLevel(uploader, slice.Encoding.Compression.GZIP.Level)
	if err != nil {
		return err
	}*/

	/*_, err = reader.WriteTo(uploader)
	if err != nil {
		return err
	}*/

	/*if errors.As(err, &pipeline.NoOpenerFoundError{}) {
		continue
	}*/

	return err
}

func (p *Plugins) SendFileImportEvent(
	ctx context.Context,
	api *keboola.AuthorizedAPI,
	duration time.Duration,
	file model.File,
	stats statistics.Value,
) error {
	var err error

	// Catch panic
	panicErr := recover()
	if panicErr != nil {
		err = errors.Errorf(`%s`, panicErr)
	}

	/*formatMsg := func(err error) string {
		if err != nil {
			return "File import failed."
		} else {
			return "File import done."
		}
	}

	err = sendEvent(ctx, api, duration, "file-import", err, formatMsg, Params{
		ProjectID: file.ProjectID,
		SourceID:  file.SourceID,
		SinkID:    file.SinkID,
		Stats:     stats,
	})*/

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}

	return err
}
