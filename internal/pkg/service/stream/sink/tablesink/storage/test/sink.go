package test

import (
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume/assignment"
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
			Config: &tablesink.ConfigPatch{
				Storage: &storage.ConfigPatch{
					VolumeAssignment: &assignment.ConfigPatch{
						Count:          Ptr(1),
						PreferredTypes: Ptr([]string{"default"}),
					},
					Local: &local.ConfigPatch{
						DiskSync: &disksync.ConfigPatch{
							Mode:            Ptr(disksync.ModeDisk),
							Wait:            Ptr(false),
							CheckInterval:   Ptr(duration.From(1 * time.Millisecond)),
							CountTrigger:    Ptr(uint(100)),
							BytesTrigger:    Ptr(100 * datasize.KB),
							IntervalTrigger: Ptr(duration.From(100 * time.Millisecond)),
						},
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
