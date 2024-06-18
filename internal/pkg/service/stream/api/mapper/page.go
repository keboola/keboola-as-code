package mapper

import (
	"context"
	"strings"

	etcd "go.etcd.io/etcd/client/v3"

	streamDesign "github.com/keboola/keboola-as-code/api/stream"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// loadPage using etcd iterator and map records to API results.
// E is type of the entity from the database.
// R is type of the mapped response.
func loadPage[E, R any](
	ctx context.Context,
	afterId string,
	limit int,
	sort etcd.SortOrder,
	list func(...iterator.Option) iterator.DefinitionT[E],
	mapper func(E) (R, error),
) (out []R, page *stream.PaginatedResponse, err error) {
	// Check limits
	if sort != etcd.SortAscend && sort != etcd.SortDescend {
		return nil, nil, errors.New(`sort must be etcd.SortAscend or etcd.SortDescend`)
	}
	if limit < streamDesign.MinPaginationLimit {
		return nil, nil, svcerrors.NewBadRequestError(
			errors.Errorf(`min pagination limit is "%d", found "%d"`, streamDesign.MinPaginationLimit, limit),
		)
	}
	if limit > streamDesign.MaxPaginationLimit {
		return nil, nil, svcerrors.NewBadRequestError(
			errors.Errorf(`max pagination limit is "%d", found "%d"`, streamDesign.MaxPaginationLimit, limit),
		)
	}

	// Fill in inputs
	page = &stream.PaginatedResponse{
		AfterID: afterId,
		Limit:   limit,
	}

	// Set sort
	opts := []iterator.Option{iterator.WithSort(etcd.SortAscend)}

	// Set offset
	switch sort {
	case etcd.SortAscend:
		opts = append(opts, iterator.WithStartOffset(afterId, false))
	case etcd.SortDescend:
		opts = append(opts, iterator.WithEndOffset(afterId, false))
	default:
		panic(errors.Errorf(`unexpected etcd sort "%v"`, sort))
	}

	// Set limit
	opts = append(opts, iterator.WithLimit(limit))

	// Create iterator
	itr := list(opts...)

	// Map each record to the response
	result := itr.
		ForEachKV(func(kv *op.KeyValueT[E], _ *iterator.Header) error {
			// Map the value
			mapped, err := mapper(kv.Value)
			if err != nil {
				return err
			}

			out = append(out, mapped)
			page.LastID = strings.TrimPrefix(kv.Key(), itr.Prefix())
			return nil
		}).
		AndOnFirstPage(func(r *etcd.GetResponse) error {
			// Clear outputs on retry
			out = nil

			// Get total count, use the list factory without options.
			// Revision is used to get the total count from the same snapshot.
			count, err := list().CountAll(etcd.WithRev(r.Header.Revision)).Do(ctx).ResultOrErr()
			if err != nil {
				return err
			}

			page.TotalCount = int(count)

			return nil
		}).
		Do(ctx)

	// Handle DB error
	if err := result.Err(); err != nil {
		return nil, nil, err
	}

	return out, page, nil
}
