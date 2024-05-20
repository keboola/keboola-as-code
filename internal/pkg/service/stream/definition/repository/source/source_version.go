package source

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
func (r *Repository) ListVersions(k key.SourceKey) iterator.DefinitionT[definition.Source] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// Version fetch entity version.
// The method can be used also for deleted objects.
func (r *Repository) Version(k key.SourceKey, version definition.VersionNumber) op.WithResult[definition.Source] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("source version", k.SourceID.String()+"/"+version.String(), "branch")
		})
}

func (r *Repository) RollbackVersion(k key.SourceKey, now time.Time, by definition.By, to definition.VersionNumber) *op.AtomicOp[definition.Source] {
	var updated definition.Source
	var latest, targetVersion *op.KeyValueT[definition.Source]
	return op.Atomic(r.client, &updated).
		// Get latest version to calculate next version number
		Read(func(ctx context.Context) op.Op {
			return r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latest)
		}).
		// Get target version
		Read(func(ctx context.Context) op.Op {
			return r.schema.Versions().Of(k).Version(to).GetKV(r.client).WithResultTo(&targetVersion)
		}).
		Write(func(ctx context.Context) op.Op {
			// Return the most significant error
			if latest == nil {
				return op.ErrorOp(serviceError.NewResourceNotFoundError("source", k.SourceID.String(), "branch"))
			} else if targetVersion == nil {
				return op.ErrorOp(serviceError.NewResourceNotFoundError("source version", k.SourceID.String()+"/"+to.String(), "branch"))
			}

			// Prepare the new value
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Value.Version.Number)
			old := targetVersion.Value
			updated = deepcopy.Copy(old).(definition.Source)
			updated.Version = latest.Value.Version
			updated.IncrementVersion(updated, now, by, versionDescription)
			return r.save(ctx, now, by, &old, &updated)
		})
}
