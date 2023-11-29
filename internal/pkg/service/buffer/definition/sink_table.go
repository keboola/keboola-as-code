package definition

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
)

const (
	SinkTypeTable = SinkType("table")
)

type TableSink struct {
	Storage *storage.ConfigPatch `json:"storage,omitempty"`
	Mapping TableMapping         `json:"mapping"`
}

type TableMapping struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
	Columns column.Columns  `json:"columns" validate:"required,min=1,max=100,dive"`
}

type StorageToken = keboola.Token
