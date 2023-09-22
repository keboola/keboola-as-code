package column

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
)

const (
	TimeFormat              = "2006-01-02T15:04:05.000Z"
	columnDatetimeType Type = "datetime"
)

type Datetime struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v Datetime) ColumnType() Type {
	return columnDatetimeType
}

func (v Datetime) ColumnName() string {
	return v.Name
}

func (v Datetime) IsPrimaryKey() bool {
	return v.PrimaryKey
}

func (v Datetime) CSVValue(ctx *receivectx.Context) (string, error) {
	// Time is always in UTC, time format has fixed length
	return ctx.Now.UTC().Format(TimeFormat), nil
}
