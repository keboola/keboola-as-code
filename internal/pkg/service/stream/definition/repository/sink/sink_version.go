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

// Version fetch entity version.
// The method can be used also for deleted objects.
func (r *Repository) Version(k key.SinkKey, version definition.VersionNumber) op.WithResult[definition.Sink] {
	return r.schema.
		Versions().Of(k).Version(version).GetOrErr(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+version.String(), "source")
		})
}

func (r *Repository) RollbackVersion(k key.SinkKey, now time.Time, by definition.By, to definition.VersionNumber) *op.AtomicOp[definition.Sink] {
	var updated definition.Sink
	var latestVersion, targetVersion *definition.Sink
	return op.Atomic(r.client, &updated).
		// Get latest version to calculate next version number
		Read(func(ctx context.Context) op.Op {
			return r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latestVersion)
		}).
		// Get target version
		Read(func(ctx context.Context) op.Op {
			return r.schema.Versions().Of(k).Version(to).GetOrNil(r.client).WithResultTo(&targetVersion)
		}).
		Write(func(ctx context.Context) op.Op {
			// Return the most significant error
			if latestVersion == nil {
				return op.ErrorOp(serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source"))
			} else if targetVersion == nil {
				return op.ErrorOp(serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+to.String(), "source"))
			}

			// Prepare the new value
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Version.Number)
			updated = deepcopy.Copy(*targetVersion).(definition.Sink)
			updated.Version = latestVersion.Version
			updated.IncrementVersion(updated, now, by, versionDescription)
			return r.save(ctx, now, by, latestVersion, &updated)
		})
}
