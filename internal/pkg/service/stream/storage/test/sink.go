package test

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
)

const (
	SinkType                 = definition.SinkType("test")
	SinkTypeWithLocalStorage = definition.SinkType("testWithLocalStorage")
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
		Type:        SinkType,
		Name:        "My Sink",
		Description: "My Description",
	}
}

func NewSinkWithLocalStorage(k key.SinkKey) definition.Sink {
	return definition.Sink{
		SinkKey:     k,
		Type:        SinkTypeWithLocalStorage,
		Name:        "My Sink",
		Description: "My Description",
	}
}

func NewKeboolaTableSink(k key.SinkKey) definition.Sink {
	return definition.Sink{
		SinkKey:     k,
		Type:        definition.SinkTypeTable,
		Name:        "My Sink",
		Description: "My Description",
		Table: &definition.TableSink{
			Type: definition.TableTypeKeboola,
			Keboola: &definition.KeboolaTable{
				TableID: keboola.MustParseTableID("in.c-bucket.table"),
			},
			Mapping: table.Mapping{
				Columns: column.Columns{
					column.Datetime{Name: "datetime"},
					column.Body{Name: "body"},
				},
			},
		},
	}
}
