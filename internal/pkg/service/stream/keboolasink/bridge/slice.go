package bridge

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (b *Bridge) uploadSlice(
	ctx context.Context,
	volume *diskreader.Volume,
	slice *model.Slice,
	alreadyUploadedSlices map[model.FileKey]string,
	stats statistics.Value,
) error {
	reader, err := volume.OpenReader(slice)
	if err != nil {
		b.logger.Warnf(ctx, "unable to open reader: %v", err)
		return err
	}

	token := b.schema.Token().ForSink(slice.SinkKey).GetOrEmpty(b.client).Do(ctx).Result()
	defer func() {
		err = reader.Close(ctx)
		if err != nil {
			return
		}

		ctx, cancel := context.WithTimeout(ctx, uploadEventSendTimeout)
		// TODO: time.Now
		err = b.sendSliceUploadEvent(ctx, b.publicAPI.WithToken(token.String()), 0, slice, stats)
		cancel()
	}()

	credentials, err := b.schema.File().ForFile(slice.FileKey).GetOrEmpty(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	uploader, err := keboola.NewUploadSliceWriter(ctx, &credentials.FileUploadCredentials, slice.String())
	if err != nil {
		return err
	}

	// Compress to GZip and measure count/size
	/*gzipWr, err := gzip.NewWriterLevel(uploader, slice.Encoding.Compression.GZIP.Level)
	if err != nil {
		return err
	}*/

	_, err = reader.WriteTo(uploader)
	if err != nil {
		return err
	}

	if err := uploader.Close(); err != nil {
		return err
	}

	alreadyUploadedSlices[slice.FileKey] = slice.String()
	uploadedSlices := make([]string, 0, len(alreadyUploadedSlices))
	for _, slice := range alreadyUploadedSlices {
		uploadedSlices = append(uploadedSlices, slice)
	}

	_, err = keboola.UploadSlicedFileManifest(ctx, &credentials.FileUploadCredentials, uploadedSlices)
	if err != nil {
		return err
	}

	return err
}
