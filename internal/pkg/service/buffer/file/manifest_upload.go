package file

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
)

func (m *AuthorizedManager) UploadManifest(ctx context.Context, resource *keboola.FileUploadCredentials, slices []model.Slice) error {
	sliceFiles := make([]string, 0)
	for _, s := range slices {
		sliceFiles = append(sliceFiles, s.Filename())
	}
	_, err := keboola.UploadSlicedFileManifest(ctx, resource, sliceFiles)
	return err
}
