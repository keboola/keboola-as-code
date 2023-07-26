package file

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func (m *AuthorizedManager) DeleteFile(ctx context.Context, file model.File) error {
	return m.projectAPI.DeleteFileRequest(file.StorageResource.ID).SendOrErr(ctx)
}
