package upload

//import (
//	"context"
//	"fmt"
//	"sync"
//	"time"
//
//	"github.com/c2h5oh/datasize"
//
//	"github.com/keboola/keboola-as-code/internal/pkg/log"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
//	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
//)
//
//const (
//	uploadSliceTimeout = 5 * time.Minute
//)
//
//func (v *Handler) watchUploading(ctx context.Context, wg *sync.WaitGroup) <-chan error {
//	return v.watchPrefix(ctx, wg, v.schema.Slices().Uploading(), func(event sliceEvent) {
//		slice := event.Value
//		lock := fmt.Sprintf("slice/upload/%s", slice.SliceKey.String())
//		v.tasks.Run(ctx, slice.ExportKey, lock, func(logger log.Logger) (string, error) {
//			// Upload should be finished even if a shutdown request is received,
//			// so we use the separated context.
//			uploadCtx, cancel := context.WithCancel(context.Background())
//			defer cancel()
//
//			// Load file with credentials
//			file, err := v.store.GetFile(uploadCtx, slice.FileKey)
//			if err != nil {
//				return "", errors.Errorf(`cannot load file "%s": %w`, slice.FileKey, err)
//			}
//
//			// Check file state
//			if file.State != filestate.Opened && file.State != filestate.Closing {
//				return "", errors.Errorf(`unexpected file "%s" state "%s", expected "opened" or "closed"`, file.FileKey, file.State)
//			}
//
//			// Log msg on shutdown request
//			go func() {
//				select {
//				case <-ctx.Done():
//					logger.Infof("waiting for the upload to complete")
//				case <-uploadCtx.Done():
//					return
//				}
//			}()
//
//			// Upload
//			var uncompressed, compressed int64
//			var uploadErr error
//			reader := v.newRecordsReader(uploadCtx, slice)
//			uploadDone := make(chan struct{})
//			go func() {
//				uncompressed, compressed, uploadErr = v.files.UploadSlice(uploadCtx, file, slice, reader)
//				close(uploadDone)
//			}()
//
//			// Wait for upload or timeout
//			select {
//			case <-v.clock.After(uploadSliceTimeout):
//				uploadErr = errors.Errorf("timeout after %s", closeSliceTimeout)
//			case <-uploadDone:
//				if uploadErr != nil {
//					uploadErr = errors.Errorf(`slice upload failed: %w`, err)
//				}
//			}
//
//			// Error
//			if uploadErr != nil {
//				if _, err := v.store.SetSliceState(ctx, &slice, slicestate.Failed); err != nil {
//					return "", err
//				}
//				return "", errors.Errorf(`slice upload failed: %w`, uploadErr)
//			}
//
//			// Success
//			if _, err := v.store.SetSliceState(ctx, &slice, slicestate.Uploaded); err != nil {
//				return "", err
//			}
//			return fmt.Sprintf(
//				`slice upload succeeded, count %d, size %s/%s`,
//				reader.Count(), datasize.ByteSize(uncompressed), datasize.ByteSize(compressed),
//			), nil
//		})
//	})
//}
