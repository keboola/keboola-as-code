package test

import (
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
)

func NewSinkKey() key.SinkKey {
	return key.SinkKey{
		SourceKey: key.SourceKey{
			BranchKey: key.BranchKey{
				ProjectID: 123,
				BranchID:  456,
			},
			SourceID: "my-source",
		},
		SinkID: "my-sink",
	}
}

func NewSink(k key.SinkKey) definition.Sink {
	return definition.Sink{
		SinkKey:     k,
		Type:        definition.SinkTypeTable,
		Name:        "My Sink",
		Description: "My Description",
		Table: &definition.TableSink{
			Storage: &storage.ConfigPatch{
				Local: &local.ConfigPatch{
					DiskSync: &disksync.Config{
						Mode:            disksync.ModeDisk,
						Wait:            false,
						CheckInterval:   1 * time.Millisecond,
						CountTrigger:    100,
						BytesTrigger:    100 * datasize.KB,
						IntervalTrigger: 100 * time.Millisecond,
					},
				},
			},
			Mapping: definition.TableMapping{
				TableID: keboola.MustParseTableID("in.bucket.table"),
				Columns: column.Columns{
					column.Datetime{Name: "datetime"},
					column.Body{Name: "body"},
				},
			},
		},
	}
}
