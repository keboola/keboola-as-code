package source

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Enable Source, and cascade enable all nested Sinks,
// if they were disabled in cascade with the Source (Switchable.Disabled.Directly == false).
func (r *Repository) Enable(k key.SourceKey, now time.Time, by definition.By) *op.AtomicOp[definition.Source] {
	var enabled definition.Source
	return op.Atomic(r.client, &enabled).
		AddFrom(r.
			enableAllFrom(k, now, by, true).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					enabled = r[0]
				}
			}))
}

func (r *Repository) enableSourcesOnBranchEnable() {
	r.plugins.Collection().OnBranchEnable(func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) error {
		op.AtomicOpFromCtx(ctx).AddFrom(r.enableAllFrom(updated.BranchKey, now, by, false))
		return nil
	})
}

// enableAllFrom the parent key.
func (r *Repository) enableAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, directly bool) *op.AtomicOp[[]definition.Source] {
	var allOriginal, allEnabled []definition.Source
	atomicOp := op.Atomic(r.client, &allEnabled)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithOnResult(func(entity definition.Source) { allOriginal = []definition.Source{entity} })
		})
	default:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.List(parentKey).WithAllTo(&allOriginal)
		})
	}

	// Iterate all
	atomicOp.Write(func(ctx context.Context) op.Op {
		txn := op.Txn(r.client)
		for _, original := range allOriginal {
			original := original

			if original.IsDisabledDirectly() != directly {
				continue
			}

			// Mark enabled
			enabled := deepcopy.Copy(original).(definition.Source)
			enabled.Enable(now, by)

			// Create a new version record, if the entity has been enabled manually
			if directly {
				enabled.IncrementVersion(enabled, now, by, "Enabled.")
			}

			// Save
			txn.Merge(r.save(ctx, now, by, &original, &enabled))
			allEnabled = append(allEnabled, enabled)
		}
		return txn
	})

	return atomicOp
}
