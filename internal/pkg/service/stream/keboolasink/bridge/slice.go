package bridge

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (b *Bridge) uploadSlice(
	ctx context.Context,
	volume *diskreader.Volume,
	slice *model.Slice,
	stats statistics.Value,
) error {
	start := time.Now()
	reader, err := volume.OpenReader(slice)
	if err != nil {
		b.logger.Warnf(ctx, "unable to open reader: %v", err)
		return err
	}

	token, err := b.schema.Token().ForSink(slice.SinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	defer func() {
		err = reader.Close(ctx)
		if err != nil {
			return
		}

		ctx, cancel := context.WithTimeout(ctx, uploadEventSendTimeout)
		err = b.sendSliceUploadEvent(ctx, b.publicAPI.WithToken(token.String()), time.Since(start), slice, stats)
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

	// uploader already contians GZip writer, so no compression is needed.
	_, err = reader.WriteTo(uploader)
	if err != nil {
		return err
	}

	if err := uploader.Close(); err != nil {
		return err
	}

	// Get already uploaded slices and update the manifest with new uploaded slice
	slices, err := b.storageRepository.Slice().ListInState(slice.FileKey, model.SliceUploaded).Do(ctx).All()
	if err != nil {
		return err
	}

	uploadedSlices := make([]string, 0, len(slices)+1)
	for _, s := range slices {
		// Skip empty slices
		if s.LocalStorage.IsEmpty {
			continue
		}

		uploadedSlices = append(uploadedSlices, s.StagingStorage.Path)
	}

	uploadedSlices = append(uploadedSlices, slice.StagingStorage.Path)
	_, err = keboola.UploadSlicedFileManifest(ctx, &credentials.FileUploadCredentials, uploadedSlices)
	if err != nil {
		return err
	}

	return err
}
