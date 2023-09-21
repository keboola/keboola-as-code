package repository

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// putMaxStatsPerTxn defines maximum number of keys per transaction when updating database values
	putMaxStatsPerTxn = 100
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
	for _, level := range storage.AllLevels() {
		txn.Then(r.schema.InLevel(level).InObject(objectKey).DeleteAll())
	}
	return txn
}

func (r *Repository) MoveOp(ctx context.Context, sliceKey storage.SliceKey, from, to storage.Level, modifyStatsFn func(*Value)) (op.Op, error) {
	if from == to {
		panic(errors.Errorf(`from and to categories are same and equal to "%s"`, to))
	}

	fromKey := r.schema.InLevel(from).InSlice(sliceKey)
	toKey := r.schema.InLevel(to).InSlice(sliceKey)

	stats, err := fromKey.Get().Do(ctx, r.client)
	if err != nil {
		return nil, err
	}

	if modifyStatsFn != nil {
		modifyStatsFn(&stats.Value)
	}

	return op.MergeToTxn(fromKey.Delete(), toKey.Put(stats.Value)), nil
}

func (r *Repository) RollupImportedOnCleanupOp(fileKey storage.FileKey) *op.AtomicOp {
	fileStatsPfx := r.schema.InLevel(storage.LevelTarget).InFile(fileKey)
	exportSumKey := r.schema.InLevel(storage.LevelTarget).InExport(fileKey.ExportKey).CleanupSum()

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

func (r *Repository) Put(ctx context.Context, stats []PerSlice) (err error) {
	ctx, span := r.telemetry.Tracer().Start(ctx, "keboola.go.buffer.storage.statistics.Repository.Put")
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
		if i == 0 || i >= putMaxStatsPerTxn {
			i = 0
			addTxn()
		}
		currentTxn.Then(r.schema.InLevel(storage.LevelLocal).InSlice(v.SliceKey).Put(v.Value))
		i++
	}

	// Trace records and transactions count
	span.SetAttributes(
		attribute.Int("statistics.put.records_count", len(stats)),
		attribute.Int("statistics.put.txn_count", len(allTxn)),
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
	return errs.ErrorOrNil()
}
