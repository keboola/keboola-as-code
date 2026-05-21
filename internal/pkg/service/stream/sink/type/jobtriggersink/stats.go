package jobtriggersink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// SinkStats contains triggered/failed job counts for a jobTrigger sink.
// Size-related fields (bytes) are always zero — jobs have no local buffering.
type SinkStats struct {
	// TriggeredCount is the total number of successfully triggered jobs.
	TriggeredCount uint64 `json:"triggeredCount"`
	// FailedCount is the total number of records that failed to trigger a job.
	FailedCount uint64 `json:"failedCount"`
	// FirstTriggeredAt is the timestamp of the first successful job trigger.
	FirstTriggeredAt utctime.UTCTime `json:"firstTriggeredAt,omitempty"`
	// LastTriggeredAt is the timestamp of the most recent successful job trigger.
	LastTriggeredAt utctime.UTCTime `json:"lastTriggeredAt,omitempty"`
}

// statsSchema is the etcd prefix for per-sink job trigger statistics.
type statsSchema struct {
	etcdop.PrefixT[SinkStats]
}

func newStatsSchema(s *serde.Serde) statsSchema {
	return statsSchema{PrefixT: etcdop.NewTypedPrefix[SinkStats]("stream/job-trigger/stats", s)}
}

func (v statsSchema) forSink(k key.SinkKey) etcdop.KeyT[SinkStats] {
	return v.Key(k.String())
}
