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
