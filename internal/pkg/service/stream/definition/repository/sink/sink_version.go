package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// ListVersions fetches all versions records for the object.
// The method can be used also for deleted objects.
func (r *Repository) ListVersions(k key.SinkKey) iterator.DefinitionT[definition.Sink] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// GetVersion fetch entity version.
// The method can be used also for deleted objects.
func (r *Repository) GetVersion(k key.SinkKey, version definition.VersionNumber) op.WithResult[definition.Sink] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+version.String(), "source")
		})
}

func (r *Repository) RollbackVersion(k key.SinkKey, now time.Time, by definition.By, to definition.VersionNumber) *op.AtomicOp[definition.Sink] {
	var updated definition.Sink
	var latestVersion, targetVersion *op.KeyValueT[definition.Sink]
	return op.Atomic(r.client, &updated).
		// Get latest version to calculate next version number
		ReadOp(r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latestVersion)).
		// Get target version
		ReadOp(r.schema.Versions().Of(k).Version(to).GetKV(r.client).WithResultTo(&targetVersion)).
		// Return the most significant error
		OnWriteOrErr(func(ctx context.Context) error {
			if latestVersion == nil {
				return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
			} else if targetVersion == nil {
				return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+to.String(), "source")
			}
			return nil
		}).
		// Prepare the new value
		Write(func(ctx context.Context) op.Op {
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Value.Version.Number)
			old := targetVersion.Value
			updated = deepcopy.Copy(old).(definition.Sink)
			updated.Version = latestVersion.Value.Version
			updated.IncrementVersion(updated, now, by, versionDescription)
			return r.save(ctx, now, by, &old, &updated)
		})
}
