package service

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func (s *service) Import(ctx context.Context, d dependencies.PublicRequestScope, payload *buffer.ImportPayload, bodyReader io.ReadCloser) (err error) {
	receiverKey := key.ReceiverKey{ProjectID: payload.ProjectID, ReceiverID: payload.ReceiverID}
	return s.importer.CreateRecord(ctx, d, receiverKey, payload.Secret, bodyReader)
}
