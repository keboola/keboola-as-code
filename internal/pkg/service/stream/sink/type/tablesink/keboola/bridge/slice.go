package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (b *Bridge) uploadSlice(ctx context.Context, volume *diskreader.Volume, slice plugin.Slice, stats statistics.Value) error {
	// Skip upload if the slice is empty.
	// The state is anyway switched to the SliceUploaded by the operator.
	if slice.LocalStorage.IsEmpty {
		b.logger.Info(ctx, "empty slice, skipped upload")
		return nil
	}

	start := time.Now()

	reader, err := volume.OpenReader(slice.SliceKey, slice.LocalStorage, slice.EncodingCompression, slice.StagingStorage.Compression)
	if err != nil {
		b.logger.Warnf(ctx, "unable to open reader: %v", err)
		return err
	}

	// Get authorization token
	existingToken, err := b.schema.Token().ForSink(slice.SinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	// Prepare encryption metadata
	metadata := cloudencrypt.Metadata{"sink": slice.SinkKey.String()}

	// Decrypt token
	token, err := existingToken.DecryptToken(ctx, b.tokenEncryptor, metadata)
	if err != nil {
		b.logger.Errorf(ctx, "cannot decrypt token: %s", err)
		if existingToken.Token == nil {
			return errors.Wrap(err, "token decryption failed and unencrypted token is missing")
		}
		token = *existingToken.Token
	}

	// Error when sending the event is not a fatal error
	defer func() {
		ctx, cancel := context.WithTimeoutCause(ctx, b.config.EventSendTimeout, errors.New("slice upload event timeout"))
		// We do not want to return err when send upload slice fails
		uErr := b.SendSliceUploadEvent(ctx, b.publicAPI.NewAuthorizedAPI(token.Token, 1*time.Minute), time.Since(start), &err, slice.SliceKey, stats)
		cancel()
		if uErr != nil {
			b.logger.Warnf(ctx, "unable to send slice upload event: %v", uErr)
			return
		}
	}()

	// Error when closing the reader is not a fatal error
	defer func() {
		err := reader.Close(ctx)
		if err != nil {
			b.logger.Warnf(ctx, "unable to close reader: %v", err)
			return
		}
	}()

	// Get file details
	keboolaFile, err := b.schema.File().ForFile(slice.FileKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	// Decrypt file upload credentials
	var credentials keboola.FileUploadCredentials
	if keboolaFile.EncryptedCredentials != "" {
		if b.credentialsEncryptor == nil {
			return errors.New("missing credentials encryptor")
		}

		fileMetadata := cloudencrypt.Metadata{"file": slice.FileKey.String()}
		credentials, err = b.credentialsEncryptor.Decrypt(ctx, []byte(keboolaFile.EncryptedCredentials), fileMetadata)
		if err != nil {
			return err
		}
	} else {
		credentials = *keboolaFile.UploadCredentials
	}

	// Upload slice
	uploader, err := keboola.NewUploadSliceWriter(ctx, &credentials, slice.StagingStorage.Path)
	if err != nil {
		return err
	}
	_, err = reader.WriteTo(uploader)
	if err != nil {
		return err
	}
	if err := uploader.Close(); err != nil {
		return err
	}

	// Update file manifest atomically, acquire the lock
	manifestLock := b.locks.NewMutex(fmt.Sprintf("upload.bridge.keboola.manifest.%s", slice.FileKey))
	if err := manifestLock.Lock(ctx); err != nil {
		b.logger.Errorf(ctx, "cannot acquire manifest lock %q: %s", manifestLock.Key(), err)
		return err
	}

	// ! Release the lock, but only after the whole operation is completed/failed, and the slice is switched to uploaded state.
	// Otherwise, uploading of another slice may overwrite the manifest record of the current slice.
	go func() {
		<-ctx.Done()

		unlockCtx, unlockCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), 10*time.Second, errors.New("manifest unlock timeout"))
		defer unlockCancel()

		if err := manifestLock.Unlock(unlockCtx); err != nil {
			b.logger.Warnf(ctx, "cannot unlock manifest lock %q: %s", manifestLock.Key(), err)
		}
	}()

	// Get already uploaded slices
	slices, err := b.storageRepository.Slice().ListInState(slice.FileKey, model.SliceUploaded).Do(ctx).All()
	if err != nil {
		return err
	}

	// Compose list of not empty uploaded slices, add the new one
	manifestSlices := make([]string, 0, len(slices)+1)
	for _, s := range slices {
		if !s.LocalStorage.IsEmpty {
			manifestSlices = append(manifestSlices, s.StagingStorage.Path)
		}
	}
	manifestSlices = append(manifestSlices, slice.StagingStorage.Path)

	// Update the manifest
	_, err = keboola.UploadSlicedFileManifest(ctx, &credentials, manifestSlices)
	return err
}
