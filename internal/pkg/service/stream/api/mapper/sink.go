package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (m *Mapper) NewSinkStatisticsTotalResponse(result statistics.Aggregated) (res *stream.SinkStatisticsTotalResult) {
	return &stream.SinkStatisticsTotalResult{
		Total: mapValueToLevel(result.Total),
		Levels: &stream.Levels{
			Local:   mapValueToLevel(result.Local),
			Staging: mapValueToLevel(result.Staging),
			Target:  mapValueToLevel(result.Target),
		},
	}
}

func mapValueToLevel(value statistics.Value) *stream.Level {
	return &stream.Level{
		FirstRecordAt:    value.FirstRecordAt.String(),
		LastRecordAt:     value.LastRecordAt.String(),
		RecordsCount:     value.RecordsCount,
		UncompressedSize: uint64(value.UncompressedSize),
	}
}
