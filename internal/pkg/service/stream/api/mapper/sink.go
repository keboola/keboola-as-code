package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (m *Mapper) NewSinkStatisticsTotalResponse(result statistics.Aggregated) *stream.SinkStatisticsTotalResult {
	res := &stream.SinkStatisticsTotalResult{
		Total: mapValueToLevel(result.Total),
		Levels: &stream.Levels{
			Local:   mapValueToLevel(result.Local),
			Staging: mapValueToLevel(result.Staging),
			Target:  mapValueToLevel(result.Target),
		},
	}

	if res.Total == nil {
		res.Total = &stream.Level{
			RecordsCount:     0,
			UncompressedSize: 0,
		}
	}

	return res
}

func (m *Mapper) NewSinkFile(file model.File) *stream.SinkFile {
	sinkFile := &stream.SinkFile{
		State:       file.State,
		OpenedAt:    file.OpenedAt().String(),
		ClosingAt:   timeToStringPointer(file.ClosingAt),
		ImportingAt: timeToStringPointer(file.ImportingAt),
		ImportedAt:  timeToStringPointer(file.ImportedAt),
	}

	if file.RetryAttempt > 0 {
		sinkFile.RetryAttempt = ptr.Ptr(file.RetryAttempt)
		sinkFile.RetryReason = ptr.Ptr(file.RetryReason)
		sinkFile.RetryAfter = ptr.Ptr(file.RetryAfter.String())
	}

	return sinkFile
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
		FirstRecordAt:    timeToStringPointer(&value.FirstRecordAt),
		LastRecordAt:     timeToStringPointer(&value.LastRecordAt),
		RecordsCount:     value.RecordsCount,
		CompressedSize:   uint64(value.CompressedSize),
		UncompressedSize: uint64(value.UncompressedSize),
	}
}

func timeToStringPointer(time *utctime.UTCTime) *string {
	if time == nil || time.IsZero() {
		return nil
	}

	return ptr.Ptr(time.String())
}
