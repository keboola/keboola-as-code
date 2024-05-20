package service

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func (s *service) AggregateSources(ctx context.Context, d dependencies.BranchRequestScope, payload *stream.AggregateSourcesPayload) (res *stream.AggregatedSourcesResult, err error) {
	return nil, errors.NewNotImplementedError()
}
