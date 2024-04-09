package branch

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"time"
)

func (r *Repository) Enable(key key.BranchKey, now time.Time, by definition.By) *op.AtomicOp[definition.Branch] {
	return r.update(key, now, by, func(branch definition.Branch) (definition.Branch, error) {
		branch.Enable(now, by)
		return branch, nil
	})

}
