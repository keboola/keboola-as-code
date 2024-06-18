package repository

import (
	"context"
	"slices"
	"strings"

	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
)

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	DefinitionRepository() *definitionRepo.Repository
	StatisticsRepository() *statsRepo.Repository
	StorageRepository() *storageRepo.Repository
}

type Repository struct {
	logger     log.Logger
	client     etcd.KV
	definition *definitionRepo.Repository
	statistics *statsRepo.Repository
	storage    *storageRepo.Repository
}

type SourceWithSinks struct {
	SourceKey key.SourceKey
	Sinks     []*SinkWithStatistics
}

type SinkWithStatistics struct {
	*definition.Sink
	Statistics *SinkStatistics
}

type SinkStatistics struct {
	Total *statistics.Aggregated
	Files []*FileWithStatistics
}

type FileWithStatistics struct {
	*model.File
	Statistics *statistics.Aggregated
}

func New(d dependencies) *Repository {
	return &Repository{
		logger:     d.Logger().WithComponent("aggregation.repository"),
		client:     d.EtcdClient(),
		definition: d.DefinitionRepository(),
		statistics: d.StatisticsRepository(),
		storage:    d.StorageRepository(),
	}
}

func (r *Repository) SourcesWithSinksAndStatistics(ctx context.Context, sourceKeys []key.SourceKey) ([]*SourceWithSinks, error) {
	sourcesWithSinks, err := r.sinksForSources(ctx, sourceKeys)
	if err != nil {
		return nil, err
	}

	err = r.addStatisticsToAggregationResponse(ctx, sourcesWithSinks)
	if err != nil {
		return nil, err
	}

	err = r.addFileStatisticsToAggregationResponse(ctx, sourcesWithSinks)
	if err != nil {
		return nil, err
	}

	return maps.Values(sourcesWithSinks), nil
}

func (r *Repository) sinksForSources(ctx context.Context, sourceKeys []key.SourceKey) (map[key.SourceKey]*SourceWithSinks, error) {
	res := make(map[key.SourceKey]*SourceWithSinks)

	txn := op.Txn(r.client)
	for _, sourceKey := range sourceKeys {
		txn.Merge(
			r.definition.Sink().List(sourceKey).ForEach(func(value definition.Sink, header *iterator.Header) error {
				source, ok := res[value.SourceKey]
				if !ok {
					source = &SourceWithSinks{
						SourceKey: value.SourceKey,
					}
					res[value.SourceKey] = source
				}
				source.Sinks = append(source.Sinks, &SinkWithStatistics{Sink: &value})
				return nil
			}),
		)
	}

	err := txn.Do(ctx).Err()

	return res, err
}

func (r *Repository) addStatisticsToAggregationResponse(ctx context.Context, res map[key.SourceKey]*SourceWithSinks) error {
	txn := op.Txn(r.client)
	for sourceKey, source := range res {
		for _, sink := range source.Sinks {
			sinkKey := key.SinkKey{
				SourceKey: sourceKey,
				SinkID:    sink.SinkID,
			}

			txn.Merge(r.statistics.AggregateIn(sinkKey).OnResult(func(result *op.TxnResult[statistics.Aggregated]) {
				sink.Statistics = &SinkStatistics{
					Total: ptr.Ptr(result.Result()),
				}
			}))

			txn.Merge(r.storage.File().ListRecentIn(sinkKey).ForEach(func(value model.File, header *iterator.Header) error {
				sink.Statistics.Files = append(sink.Statistics.Files, &FileWithStatistics{
					File: ptr.Ptr(value),
				})
				return nil
			}))
		}
	}

	return txn.Do(ctx).Err()
}

func (r *Repository) addFileStatisticsToAggregationResponse(ctx context.Context, res map[key.SourceKey]*SourceWithSinks) error {
	txn := op.Txn(r.client)
	for sourceKey, source := range res {
		for _, sink := range source.Sinks {
			if len(sink.Statistics.Files) == 0 {
				continue
			}

			sinkKey := key.SinkKey{
				SourceKey: sourceKey,
				SinkID:    sink.SinkID,
			}

			filesMap := make(map[string]*FileWithStatistics)
			for _, file := range sink.Statistics.Files {
				filesMap[file.FileID.String()] = file
			}

			// Sort keys lexicographically
			keys := maps.Keys(filesMap)
			slices.SortStableFunc(keys, strings.Compare)

			txn.Merge(
				r.statistics.FilesStats(
					sinkKey,
					model.FileID{OpenedAt: utctime.MustParse(keys[0])},
					model.FileID{OpenedAt: utctime.MustParse(keys[len(keys)-1])},
				).OnResult(func(result *op.TxnResult[map[model.FileID]*statistics.Aggregated]) {
					for fileID, aggregated := range result.Result() {
						filesMap[fileID.String()].Statistics = aggregated
					}
				}),
			)
		}
	}

	return txn.Do(ctx).Err()
}
