package definition

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
)

const (
	SinkTypeTable    = SinkType("table")
	TableTypeKeboola = TableType("keboola")
)

type TableType string

// TableSink configures destination table.
type TableSink struct {
	Type    TableType     `json:"type" validate:"required,oneof=keboola"`
	Keboola *KeboolaTable `json:"keboola" validate:"required_if=Type keboola"`

	Mapping table.Mapping `json:"mapping"`
}

type KeboolaTable struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
}

func (t TableType) String() string {
	return string(t)
}
