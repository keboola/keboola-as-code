package file

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func (m *AuthorizedManager) UploadManifest(ctx context.Context, resource *keboola.FileUploadCredentials, slices []model.Slice) error {
	sliceFiles := make([]string, 0)
	for _, s := range slices {
		sliceFiles = append(sliceFiles, s.Filename())
	}
	_, err := keboola.UploadSlicedFileManifest(ctx, resource, sliceFiles)
	return err
}
