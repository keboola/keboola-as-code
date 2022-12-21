package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
)

func (s *service) RefreshReceiverTokens(d dependencies.ForProjectRequest, payload *buffer.RefreshReceiverTokensPayload) (res *buffer.Receiver, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	rb := rollback.New(s.logger)
	defer rb.InvokeIfErr(ctx, &err)

	receiverKey := key.ReceiverKey{ProjectID: key.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	tokens, err := str.ListTokens(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	if err := d.TokenManager().RefreshTokens(ctx, rb, tokens); err != nil {
		return nil, err
	}

	if err = str.UpdateTokens(ctx, tokens); err != nil {
		return nil, err
	}

	return s.GetReceiver(d, &buffer.GetReceiverPayload{ReceiverID: payload.ReceiverID})
}
