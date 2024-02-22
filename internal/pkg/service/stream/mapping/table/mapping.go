// Package table provides mapping of incoming data to table format.
package table

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"

type Mapping struct {
	Columns column.Columns `json:"columns" validate:"required,min=1,max=100,dive"`
}
