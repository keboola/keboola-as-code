package statistics

import (
	"context"
	"fmt"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type atomicProvider = AtomicProvider

type Repository struct {
	*atomicProvider
	telemetry telemetry.Telemetry
	client    *etcd.Client
	schema    schemaRoot
}

type repositoryDeps interface {
	Telemetry() telemetry.Telemetry
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewRepository(provider *AtomicProvider, d repositoryDeps) *Repository {
	return &Repository{
		atomicProvider: provider,
		telemetry:      d.Telemetry(),
		client:         d.EtcdClient(),
		schema:         newSchema(d.EtcdSerde()),
	}
}

func (r *Repository) AtomicProvider() *AtomicProvider {
	return r.atomicProvider
}

func (r *Repository) DeleteOp(objectKey fmt.Stringer) op.Op {
	txn := op.NewTxnOp()
	for _, category := range allCategories {
		txn.Then(r.schema.InCategory(category).InObject(objectKey).DeleteAll())
	}
	return txn
}

func (r *Repository) MoveOp(ctx context.Context, sliceKey key.SliceKey, from, to Category, modifyStatsFn func(*Value)) (op.Op, error) {
	if from == to {
		panic(errors.Errorf(`from and to categories are same and equal to "%s"`, to))
	}

	fromPfx := r.schema.InCategory(from).InSlice(sliceKey)
	toKey := r.schema.InCategory(to).InSlice(sliceKey).NodesSum()

	stats, err := SumStats(ctx, r.client, fromPfx.GetAll())
	if err != nil {
		return nil, err
	}

	if modifyStatsFn != nil {
		modifyStatsFn(&stats)
	}

	txn := op.NewTxnOp()
	txn.Then(fromPfx.DeleteAll())

	if stats.RecordsCount > 0 {
		txn.Then(toKey.Put(stats))
	}

	return txn, nil
}

func (r *Repository) RollupImportedOnCleanupOp(fileKey key.FileKey) *op.AtomicOp {
	fileStatsPfx := r.schema.InCategory(Imported).InFile(fileKey)
	exportSumKey := r.schema.InCategory(Imported).InExport(fileKey.ExportKey).CleanupSum()

	var sumValue Value
	var sliceValue Value
	return op.
		Atomic().
		Read(func() op.Op {
			// Get export sum
			return exportSumKey.Get().WithOnResult(func(result *op.KeyValueT[Value]) {
				if result != nil {
					sumValue = result.Value
				}
			})
		}).
		Read(func() op.Op {
			// Get file stats
			return SumStatsOp(fileStatsPfx.GetAll(), &sliceValue)
		}).
		Write(func() op.Op {
			// Sum both values and save
			return exportSumKey.Put(sumValue.Add(sliceValue))
		}).
		Write(func() op.Op {
			// Delete file stats
			return fileStatsPfx.DeleteAll()
		})
}

func (r *Repository) Insert(ctx context.Context, nodeID string, stats []PerAPINode) (err error) {
	ctx, span := r.telemetry.Tracer().Start(ctx, "keboola.go.buffer.statistics.Repository.Insert")
	defer span.End(&err)

	var currentTxn *op.TxnOp
	var allTxn []*op.TxnOp
	addTxn := func() {
		currentTxn = op.NewTxnOp()
		allTxn = append(allTxn, currentTxn)
	}

	// Merge multiple put operations into one transaction
	i := 0
	for _, v := range stats {
		if i == 0 || i >= collectorMaxStatsPerTxn {
			i = 0
			addTxn()
		}
		currentTxn.Then(r.schema.InCategory(Buffered).InSlice(v.SliceKey).PerNode(nodeID).Put(v.Value))
		i++
	}

	// Trace records and transactions count
	span.SetAttributes(
		attribute.Int("statistics.collector.records_count", len(stats)),
		attribute.Int("statistics.collector.txn_count", len(allTxn)),
	)

	// Run transactions in parallel
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for _, txn := range allTxn {
		txn := txn
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := txn.Do(ctx, r.client); err != nil {
				errs.Append(err)
			}
		}()
	}

	// Wait for all transactions
	wg.Wait()
	if err := errs.ErrorOrNil(); err != nil {
		return err
	}

	return nil
}

func SliceStateToCategory(s slicestate.State) Category {
	switch s {
	case slicestate.Writing, slicestate.Closing, slicestate.Uploading, slicestate.Failed:
		return Buffered
	case slicestate.Uploaded:
		return Uploaded
	case slicestate.Imported:
		return Imported
	default:
		panic(errors.Errorf(`unexpected slice state "%v"`, s))
	}
}

func SumStats(ctx context.Context, client *etcd.Client, prefix iterator.DefinitionT[Value]) (out Value, err error) {
	if err := SumStatsOp(prefix, &out).DoOrErr(ctx, client); err != nil {
		return out, err
	}
	return out, nil
}

// SumStatsOp sums all stats from the iterator.
func SumStatsOp(prefix iterator.DefinitionT[Value], out *Value) *iterator.ForEachOpT[Value] {
	return prefix.ForEachOp(func(item Value, _ *iterator.Header) error {
		*out = out.Add(item)
		return nil
	})
}
