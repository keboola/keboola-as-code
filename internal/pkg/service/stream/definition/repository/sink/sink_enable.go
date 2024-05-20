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

// Enable Sink.
func (r *Repository) Enable(k key.SinkKey, now time.Time, by definition.By) *op.AtomicOp[definition.Sink] {
	var enabled definition.Sink
	return op.Atomic(r.client, &enabled).
		AddFrom(r.
			enableAllFrom(k, now, by, true).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					enabled = r[0]
				}
			}))
}

func (r *Repository) enableSinksOnSourceEnable() {
	r.plugins.Collection().OnSourceSave(func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) error {
		if updated.IsEnabledAt(now) {
			op.AtomicFromCtx(ctx).AddFrom(r.enableAllFrom(updated.SourceKey, now, by, false))
		}
		return nil
	})
}

// enableAllFrom the parent key.
func (r *Repository) enableAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, directly bool) *op.AtomicOp[[]definition.Sink] {
	var allOriginal, allEnabled []definition.Sink
	atomicOp := op.Atomic(r.client, &allEnabled)

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
			if original.IsDisabledDirectly() != directly {
				continue
			}

			// Mark enabled
			enabled := deepcopy.Copy(original).(definition.Sink)
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
