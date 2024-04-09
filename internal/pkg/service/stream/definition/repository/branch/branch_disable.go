package branch

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"time"
)

func (r *Repository) Disable(key key.BranchKey, now time.Time, by definition.By, reason string) *op.AtomicOp[definition.Branch] {
	return r.update(key, now, by, func(branch definition.Branch) (definition.Branch, error) {
		branch.Disable(now, by, reason, true)
		return branch, nil
	})

}
