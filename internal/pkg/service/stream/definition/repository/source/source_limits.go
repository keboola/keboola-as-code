package source

import (
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

const (
	MaxSourcesPerBranch        = 100
	MaxSourceVersionsPerSource = 1000
)

func (r *Repository) checkMaxSourcesPerBranch(k key.BranchKey, newCount int64) op.Op {
	return r.schema.
		Active().InBranch(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSourcesPerBranch {
				return serviceError.NewCountLimitReachedError("source", MaxSourcesPerBranch, "branch")
			}
			return nil
		})
}

func (r *Repository) checkMaxSourcesVersionsPerSource(k key.SourceKey, newCount int64) op.Op {
	return r.schema.
		Versions().Of(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSourceVersionsPerSource {
				return serviceError.NewCountLimitReachedError("version", MaxSourceVersionsPerSource, "source")
			}
			return nil
		})
}
