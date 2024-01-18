// Package column provides all column types used in the Buffer API.
//
// # Add a new column type "Foo"
//
// - Create a file for it: `foo.go`.
// - Create a struct for it: `type Foo struct{name string; ...}`.
// - Create a type constant for it: `columnFooType Type = "foo"`.
// - Add it to `MakeColumn` function.
// - Implement the `Columnss` interface for the `ColumnFoo` struct.
package column

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Type string

// Column is an interface used to restrict valid column types.
type Column interface {
	ColumnName() string
	ColumnType() Type
	IsPrimaryKey() bool
	CSVValue(ctx *receivectx.Context) (string, error)
}

func MakeColumn(typ Type, name string, primaryKey bool) (Column, error) {
	var v Column
	switch typ {
	case columnIDType:
		v = ID{Name: name, PrimaryKey: primaryKey}
	case columnDatetimeType:
		v = Datetime{Name: name, PrimaryKey: primaryKey}
	case columnIPType:
		v = IP{Name: name, PrimaryKey: primaryKey}
	case columnBodyType:
		v = Body{Name: name, PrimaryKey: primaryKey}
	case columnHeadersType:
		v = Headers{Name: name, PrimaryKey: primaryKey}
	case columnTemplateType:
		v = Template{Name: name, PrimaryKey: primaryKey}
	default:
		return nil, errors.Errorf(`invalid column type "%s"`, typ)
	}
	return v, nil
}
