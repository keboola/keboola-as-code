package definition

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type SinkType string

type Sink struct {
	key.SinkKey
	Created
	Versioned
	Switchable
	SoftDeletable
	Type        SinkType             `json:"type" validate:"required"`
	Name        string               `json:"name" validate:"required,min=1,max=40"`
	Description string               `json:"description,omitempty" validate:"max=4096"`
	Config      configpatch.PatchKVs `json:"config,omitempty"` // see stream/config/config.Patch

	// AllowedSignals filters which OTLP signal types this sink accepts.
	// Empty means accept all records (HTTP sources and all OTLP signals).
	// Valid values: "logs", "metrics", "traces".
	AllowedSignals []string `json:"allowedSignals,omitempty" validate:"dive,oneof=logs metrics traces"`

	// Sink type specific fields

	Table *TableSink `json:"table,omitempty" validate:"required_if=Type table"`
}

func (t SinkType) String() string {
	return string(t)
}

// AcceptsSignal returns true when a record carrying the given OTLP signal
// should be dispatched to this sink — convenience wrapper around
// SignalAccepted using the sink's own AllowedSignals.
func (s *Sink) AcceptsSignal(signal string) bool {
	return SignalAccepted(s.AllowedSignals, signal)
}

// SignalAccepted returns true when a record carrying the given OTLP signal
// should be dispatched to a sink with the given AllowedSignals filter.
//
// The empty signal (HTTP-source records) bypasses the filter so HTTP sources
// ignore AllowedSignals as documented, even when a sink is shared with OTLP.
// An empty AllowedSignals on an OTLP record accepts every signal.
//
// This is the single source of truth used by the runtime router and the
// /test endpoint — keep both paths in sync by routing through here.
func SignalAccepted(allowedSignals []string, signal string) bool {
	if signal == "" {
		return true
	}
	if len(allowedSignals) == 0 {
		return true
	}
	for _, allowed := range allowedSignals {
		if allowed == signal {
			return true
		}
	}
	return false
}
