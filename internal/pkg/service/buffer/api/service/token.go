package service

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
)

func (s *service) RefreshReceiverTokens(ctx context.Context, d dependencies.ProjectRequestScope, payload *buffer.RefreshReceiverTokensPayload) (res *buffer.Receiver, err error) {
	str := d.Store()

	rb := rollback.New(s.logger)
	defer rb.InvokeIfErr(ctx, &err)

	receiverKey := key.ReceiverKey{ProjectID: d.ProjectID(), ReceiverID: payload.ReceiverID}
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

	return s.GetReceiver(ctx, d, &buffer.GetReceiverPayload{ReceiverID: payload.ReceiverID})
}
