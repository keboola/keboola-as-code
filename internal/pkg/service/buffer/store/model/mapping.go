package model

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
)

type Mapping struct {
	key.MappingKey
	TableID     keboola.TableID `json:"tableId" validate:"required"`
	Incremental bool            `json:"incremental"`
	Columns     column.Columns  `json:"columns" validate:"required,min=1,max=100"`
}
