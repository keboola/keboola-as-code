package column

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
)

const (
	columnBodyType Type = "body"
)

type Body struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v Body) ColumnType() Type {
	return columnBodyType
}

func (v Body) ColumnName() string {
	return v.Name
}

func (v Body) IsPrimaryKey() bool {
	return v.PrimaryKey
}

func (Body) CSVValue(ctx *receivectx.Context) (string, error) {
	return ctx.Body, nil
}
