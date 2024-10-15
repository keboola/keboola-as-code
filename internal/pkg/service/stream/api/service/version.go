package service

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (s *service) versionMustExist(ctx context.Context, k key.SourceKey, number definition.VersionNumber) error {
	return s.definition.Source().Version(k, number).Do(ctx).Err()
}
