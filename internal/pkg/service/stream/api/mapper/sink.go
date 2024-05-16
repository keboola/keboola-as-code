package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
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
	if value.RecordsCount == 0 {
		return nil
	}

	return &stream.Level{
		FirstRecordAt:    timeValueToPointer(value.FirstRecordAt),
		LastRecordAt:     timeValueToPointer(value.LastRecordAt),
		RecordsCount:     value.RecordsCount,
		UncompressedSize: uint64(value.UncompressedSize),
	}
}

func timeValueToPointer(time utctime.UTCTime) *string {
	var result *string
	if !time.IsZero() {
		value := time.String()
		result = &value
	}

	return result
}
