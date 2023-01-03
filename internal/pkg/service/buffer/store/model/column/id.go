package column

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
)

const (
	IDPlaceholder      = "<<~~id~~>>"
	columnIDType  Type = "id"
)

type ID struct {
	Name string `json:"name" validate:"required"`
}

func (v ID) ColumnType() Type {
	return columnIDType
}

func (v ID) ColumnName() string {
	return v.Name
}

func (ID) CSVValue(_ *receivectx.Context) (string, error) {
	return IDPlaceholder, nil
}
