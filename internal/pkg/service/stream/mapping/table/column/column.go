// Package column provides all column types used in the Buffer API.
//
// # Add a new column type "Foo"
//
// - Create a file for it: `foo.go`.
// - Create a struct for it: `type Foo struct{name string; ...}`.
// - Create a type constant for it: `columnFooType Type = "foo"`.
// - Add it to `MakeColumn` function.
// - Implement the `Column` interface for the `ColumnFoo` struct.
package column

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Type string

type Types []Type

// Column is an interface used to restrict valid column types.
type Column interface {
	ColumnName() string
	ColumnType() Type
	IsPrimaryKey() bool
}

func AllTypes() Types {
	return Types{
		ColumnUUIDType,
		ColumnDatetimeType,
		ColumnIPType,
		ColumnBodyType,
		ColumnHeadersType,
		ColumnTemplateType,
	}
}

func MakeColumn(typ Type, name string, primaryKey bool) (Column, error) {
	var v Column
	switch typ {
	case ColumnUUIDType:
		v = UUID{Name: name, PrimaryKey: primaryKey}
	case ColumnDatetimeType:
		v = Datetime{Name: name, PrimaryKey: primaryKey}
	case ColumnIPType:
		v = IP{Name: name, PrimaryKey: primaryKey}
	case ColumnBodyType:
		v = Body{Name: name, PrimaryKey: primaryKey}
	case ColumnHeadersType:
		v = Headers{Name: name, PrimaryKey: primaryKey}
	case ColumnTemplateType:
		v = Template{Name: name, PrimaryKey: primaryKey}
	default:
		return nil, errors.Errorf(`invalid column type "%s"`, typ)
	}
	return v, nil
}

func (v Type) String() string {
	return string(v)
}

func (v Types) Strings() (out []string) {
	for _, t := range v {
		out = append(out, t.String())
	}
	return out
}

func (v Types) AnySlice() (out []any) {
	for _, t := range v {
		out = append(out, t.String())
	}
	return out
}
