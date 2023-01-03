package column

import (
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
)

const (
	columnHeadersType Type = "headers"
)

type Headers struct {
	Name string `json:"name" validate:"required"`
}

func (v Headers) ColumnType() Type {
	return columnHeadersType
}

func (v Headers) ColumnName() string {
	return v.Name
}

func (Headers) CSVValue(ctx *receivectx.Context) (string, error) {
	return json.EncodeString(ctx.HeadersMap(), false)
}
