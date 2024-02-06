package definition

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type SinkType string

type Sink struct {
	key.SinkKey
	Versioned
	Switchable
	SoftDeletable
	Type        SinkType   `json:"type" validate:"required,oneof=table"`
	Name        string     `json:"name" validate:"required,min=1,max=40"`
	Description string     `json:"description,omitempty" validate:"max=4096"`
	Table       *TableSink `json:"table" validate:"required_if=Type table"`
}

func (t SinkType) String() string {
	return string(t)
}
