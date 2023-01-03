package column

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
)

const (
	columnIPType Type = "ip"
)

type IP struct {
	Name string `json:"name" validate:"required"`
}

func (v IP) ColumnType() Type {
	return columnIPType
}

func (v IP) ColumnName() string {
	return v.Name
}

func (IP) CSVValue(ctx *receivectx.Context) (string, error) {
	return ctx.IP.String(), nil
}
