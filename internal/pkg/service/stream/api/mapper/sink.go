package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (m *Mapper) NewSinkStatisticsTotalResponse(result statistics.Aggregated) *stream.SinkStatisticsTotalResult {
	return &stream.SinkStatisticsTotalResult{
		Total: mapValueToLevel(result.Total),
		Levels: &stream.Levels{
			Local:   mapValueToLevel(result.Local),
			Staging: mapValueToLevel(result.Staging),
			Target:  mapValueToLevel(result.Target),
		},
	}
}

func (m *Mapper) NewSinkFile(file model.File) *stream.SinkFile {
	return &stream.SinkFile{
		State:       file.State,
		OpenedAt:    file.OpenedAt().String(),
		ClosingAt:   timeToString(file.ClosingAt),
		ImportingAt: timeToString(file.ImportingAt),
		ImportedAt:  timeToString(file.ImportedAt),
	}
}

func (m *Mapper) NewSinkFileStatistics(result *statistics.Aggregated) *stream.SinkFileStatistics {
	return &stream.SinkFileStatistics{
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
		FirstRecordAt:    timeToString(&value.FirstRecordAt),
		LastRecordAt:     timeToString(&value.LastRecordAt),
		RecordsCount:     value.RecordsCount,
		UncompressedSize: uint64(value.UncompressedSize),
	}
}

func timeToString(time *utctime.UTCTime) *string {
	if time == nil || time.IsZero() {
		return nil
	}

	return ptr.Ptr(time.String())
}
