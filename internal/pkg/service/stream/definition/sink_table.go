package definition

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
)

const (
	SinkTypeTable = SinkType("table")
)

// TableSink configures destination table.
type TableSink struct {
	Keboola TableSinkKeboola `json:"keboola"`
	Mapping table.Mapping    `json:"mapping"`
}

type TableSinkConfig struct {
	Storage *storage.ConfigPatch `json:"storage,omitempty"`
}

type TableSinkMapping struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
	Columns column.Columns  `json:"columns" validate:"required,min=1,max=100,dive"`
}

type StorageToken = keboola.Token
