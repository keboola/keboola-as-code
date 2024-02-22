package definition

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
)

const (
	SinkTypeTable = SinkType("table")
)

// TableSink configures destination table.
type TableSink struct {
	Keboola TableSinkKeboola `json:"keboola"`
	Mapping table.Mapping    `json:"mapping"`
}

type TableSinkKeboola struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
}
