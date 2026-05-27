package service

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func keboolaTableSink(branchID keboola.BranchID, tableID string) definition.Sink {
	return definition.Sink{
		SinkKey: key.SinkKey{SourceKey: key.SourceKey{BranchKey: key.BranchKey{BranchID: branchID}}},
		Type:    definition.SinkTypeTable,
		Table: &definition.TableSink{
			Type:    definition.TableTypeKeboola,
			Keboola: &definition.KeboolaTable{TableID: keboola.MustParseTableID(tableID)},
		},
	}
}

func TestCollectKeboolaTableResources(t *testing.T) {
	t.Parallel()

	const branchID = keboola.BranchID(456)

	// Two tables sharing one OTLP bucket, one table in another bucket, plus sinks that own no
	// Keboola table and must be ignored.
	sinks := []definition.Sink{
		keboolaTableSink(branchID, "in.c-otlp-my-source.logs"),
		keboolaTableSink(branchID, "in.c-otlp-my-source.metrics"),
		keboolaTableSink(branchID, "in.c-http-my-source.records"),
		{Type: definition.SinkTypeTable, Table: nil},                                                        // table sink without config
		{Type: definition.SinkType("router")},                                                               // non-table sink
		{Type: definition.SinkTypeTable, Table: &definition.TableSink{Type: definition.TableType("other")}}, // non-Keboola table
	}

	tableKeys, bucketKeys := collectKeboolaTableResources(sinks)

	// All three Keboola tables are collected, in sink order.
	assert.Equal(t, []keboola.TableKey{
		{BranchID: branchID, TableID: keboola.MustParseTableID("in.c-otlp-my-source.logs")},
		{BranchID: branchID, TableID: keboola.MustParseTableID("in.c-otlp-my-source.metrics")},
		{BranchID: branchID, TableID: keboola.MustParseTableID("in.c-http-my-source.records")},
	}, tableKeys)

	// Buckets are deduplicated (the two OTLP tables share one bucket).
	assert.Equal(t, []keboola.BucketKey{
		{BranchID: branchID, BucketID: keboola.MustParseTableID("in.c-otlp-my-source.logs").BucketID},
		{BranchID: branchID, BucketID: keboola.MustParseTableID("in.c-http-my-source.records").BucketID},
	}, bucketKeys)
}

func TestCollectKeboolaTableResources_NoKeboolaSinks(t *testing.T) {
	t.Parallel()

	tableKeys, bucketKeys := collectKeboolaTableResources([]definition.Sink{
		{Type: definition.SinkType("router")},
	})
	assert.Empty(t, tableKeys)
	assert.Empty(t, bucketKeys)
}

func TestIsResourceNotFound(t *testing.T) {
	t.Parallel()

	assert.True(t, isResourceNotFound(&keboola.StorageError{ErrCode: "storage.tables.notFound"}))
	assert.True(t, isResourceNotFound(&keboola.StorageError{ErrCode: "storage.buckets.notFound"}))
	// Wrapped error is still detected.
	assert.True(t, isResourceNotFound(errors.Errorf("cannot delete: %w", &keboola.StorageError{ErrCode: "storage.buckets.notFound"})))
	// Other Storage errors are not treated as "already deleted".
	assert.False(t, isResourceNotFound(&keboola.StorageError{ErrCode: "storage.tables.notEmpty"}))
	assert.False(t, isResourceNotFound(errors.New("network error")))
}
