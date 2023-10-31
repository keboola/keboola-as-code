package file

import (
	"context"
)

func (m *AuthorizedManager) DeleteFile(ctx context.Context, file model.File) error {
	return m.projectAPI.DeleteFileRequest(file.StorageResource.ID).SendOrErr(ctx)
}
