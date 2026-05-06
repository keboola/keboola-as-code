package kaiagentsink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// SinkStats contains sent/failed request counts for a kaiAgent sink.
type SinkStats struct {
	SentCount   uint64          `json:"sentCount"`
	FailedCount uint64          `json:"failedCount"`
	FirstSentAt utctime.UTCTime `json:"firstSentAt,omitempty"`
	LastSentAt  utctime.UTCTime `json:"lastSentAt,omitempty"`
}

type statsSchema struct {
	etcdop.PrefixT[SinkStats]
}

func newStatsSchema(s *serde.Serde) statsSchema {
	return statsSchema{PrefixT: etcdop.NewTypedPrefix[SinkStats]("stream/kai-agent/stats", s)}
}

func (v statsSchema) forSink(k key.SinkKey) etcdop.KeyT[SinkStats] {
	return v.Key(k.String())
}
