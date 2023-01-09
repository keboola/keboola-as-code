package service

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func (s *service) Import(d dependencies.ForPublicRequest, payload *buffer.ImportPayload, bodyReader io.ReadCloser) (err error) {
	ctx := d.RequestCtx()
	receiverKey := key.ReceiverKey{ProjectID: payload.ProjectID, ReceiverID: payload.ReceiverID}
	return s.importer.CreateRecord(ctx, d, receiverKey, payload.Secret, bodyReader)
}
