package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Disable Sink.
func (r *Repository) Disable(k key.SinkKey, now time.Time, by definition.By, reason string) *op.AtomicOp[definition.Sink] {
	var enabled definition.Sink
	return op.Atomic(r.client, &enabled).
		AddFrom(r.
			disableAllFrom(k, now, by, reason, true).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					enabled = r[0]
				}
			}))
}

func (r *Repository) disableSinksOnSourceDisable() {
	r.plugins.Collection().OnSourceDisabled(func(ctx context.Context, now time.Time, by definition.By, original, updated *definition.Source) error {
		reason := "Auto-disabled with the parent source."
		op.AtomicOpCtxFrom(ctx).AddFrom(r.disableAllFrom(updated.SourceKey, now, by, reason, false))
		return nil
	})
}

// disableAllFrom the parent key.
func (r *Repository) disableAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, reason string, directly bool) *op.AtomicOp[[]definition.Sink] {
	var allOriginal, allDisabled []definition.Sink
	atomicOp := op.Atomic(r.client, &allDisabled)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithOnResult(func(entity definition.Sink) { allOriginal = []definition.Sink{entity} })
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
			if original.IsDisabled() {
				continue
			}

			// Mark disabled
			disabled := deepcopy.Copy(original).(definition.Sink)
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
