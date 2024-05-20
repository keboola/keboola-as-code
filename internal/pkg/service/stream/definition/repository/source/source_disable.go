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

// Disable Source, and cascade disable all nested Sinks.
func (r *Repository) Disable(k key.SourceKey, now time.Time, by definition.By, reason string) *op.AtomicOp[definition.Source] {
	var enabled definition.Source
	return op.Atomic(r.client, &enabled).
		AddFrom(r.
			disableAllFrom(k, now, by, reason, true).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					enabled = r[0]
				}
			}))
}

func (r *Repository) disableSourcesOnBranchDisable() {
	r.plugins.Collection().OnBranchSave(func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Branch) error {
		if updated.IsDisabledAt(now) {
			reason := "Auto-disabled with the parent branch."
			op.AtomicFromCtx(ctx).AddFrom(r.disableAllFrom(updated.BranchKey, now, by, reason, false))
		}
		return nil
	})
}

// disableAllFrom the parent key.
func (r *Repository) disableAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, reason string, directly bool) *op.AtomicOp[[]definition.Source] {
	var allOriginal, allDisabled []definition.Source
	atomicOp := op.Atomic(r.client, &allDisabled)

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

			if original.IsDisabled() {
				continue
			}

			// Mark disabled
			disabled := deepcopy.Copy(original).(definition.Source)
			disabled.Disable(now, by, reason, directly)

			// Create a new version record, if the entity has been disabled manually
			if directly {
				disabled.IncrementVersion(disabled, now, by, "Disabled.")
			}

			// Save
			txn.Merge(r.save(ctx, now, by, &original, &disabled))
			allDisabled = append(allDisabled, disabled)
		}
		return txn
	})

	return atomicOp
}
