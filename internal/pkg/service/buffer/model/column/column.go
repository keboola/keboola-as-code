package column

import (
	"encoding/json"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// To add a new column type `Foo`:
// - create a struct for it: `type Foo struct{}`
// - create a type name constant for it: `columnIDFoo = "foo"`
// - add it to `columnToType` and `typeToColumn`
// - implement `Column` for `ColumnFoo`

type Columns []Column

type (
	ID       struct{}
	Datetime struct{}
	IP       struct{}
	Body     struct{}
	Header   struct{}
)

const (
	IDPlaceholder               = "<<~~id~~>>"
	UndefinedValueStrategyNull  = "null"
	UndefinedValueStrategyError = "error"
)

type Template struct {
	Language               string `json:"language" validate:"required,oneof=jsonnet"`
	UndefinedValueStrategy string `json:"undefinedValueStrategy" validate:"required,oneof=null error"`
	Content                string `json:"content" validate:"required,min=1,max=4096"`
	DataType               string `json:"dataType" validate:"required,oneof=STRING INTEGER NUMERIC FLOAT BOOLEAN DATE TIMESTAMP"`
}

const (
	columnIDType       = "id"
	columnDatetimeType = "datetime"
	columnIPType       = "ip"
	columnBodyType     = "body"
	columnHeadersType  = "headers"
	columnTemplateType = "template"
)

func (v Columns) MarshalJSON() ([]byte, error) {
	var items []json.RawMessage

	for _, column := range v {
		column := column

		typ, err := ColumnToType(column)
		if err != nil {
			return nil, err
		}

		typeJson, err := json.Marshal(typ)
		if err != nil {
			return nil, err
		}

		columnJson, err := json.Marshal(&column)
		if err != nil {
			return nil, err
		}
		columnJson = columnJson[1 : len(columnJson)-1]

		item := json.RawMessage(`{"type":`)
		item = append(item, typeJson...)
		if len(columnJson) > 0 {
			item = append(item, byte(','))
			item = append(item, columnJson...)
		}
		item = append(item, byte('}'))

		items = append(items, item)
	}

	return json.Marshal(items)
}

func (v *Columns) UnmarshalJSON(b []byte) error {
	*v = nil

	var items []json.RawMessage
	if err := json.Unmarshal(b, &items); err != nil {
		return err
	}

	for _, item := range items {
		t := struct {
			Type string `json:"type"`
		}{}
		if err := json.Unmarshal(item, &t); err != nil {
			return err
		}

		data, err := TypeToColumn(t.Type)
		if err != nil {
			return err
		}

		ptr := reflect.New(reflect.TypeOf(data))
		ptr.Elem().Set(reflect.ValueOf(data))

		if err = json.Unmarshal(item, ptr.Interface()); err != nil {
			return err
		}

		*v = append(*v, ptr.Elem().Interface().(Column))
	}

	return nil
}

// ColumnToType returns the stringified type of the column.
//
// This function expects `column` to be passed by value.
func ColumnToType(column any) (string, error) {
	switch column.(type) {
	case ID:
		return columnIDType, nil
	case Datetime:
		return columnDatetimeType, nil
	case IP:
		return columnIPType, nil
	case Body:
		return columnBodyType, nil
	case Header:
		return columnHeadersType, nil
	case Template:
		return columnTemplateType, nil
	default:
		return "", errors.Errorf(`invalid column type "%T"`, column)
	}
}

// ColumnToType returns the stringified type of the column.
//
// This function returns `column` as a value.
func TypeToColumn(typ string) (Column, error) {
	switch typ {
	case columnIDType:
		return ID{}, nil
	case columnDatetimeType:
		return Datetime{}, nil
	case columnIPType:
		return IP{}, nil
	case columnBodyType:
		return Body{}, nil
	case columnHeadersType:
		return Header{}, nil
	case columnTemplateType:
		return Template{}, nil
	default:
		return dummyColumn{}, errors.Errorf(`invalid column type name "%s"`, typ)
	}
}

type ImportCtx struct {
	Body     *orderedmap.OrderedMap
	DateTime time.Time
	Header   http.Header
	IP       net.IP
}

func NewImportCtx(body *orderedmap.OrderedMap, header http.Header, ip net.IP) ImportCtx {
	return ImportCtx{
		Body:     body,
		DateTime: time.Now(),
		Header:   header,
		IP:       ip,
	}
}

// Column is an interface used to restrict valid column types.
type Column interface {
	CsvValue(importCtx ImportCtx) (string, error)
}

func (ID) CsvValue(_ ImportCtx) (string, error) {
	return IDPlaceholder, nil
}

func (Datetime) CsvValue(importCtx ImportCtx) (string, error) {
	return importCtx.DateTime.Format(time.RFC3339), nil
}

func (IP) CsvValue(importCtx ImportCtx) (string, error) {
	return importCtx.IP.String(), nil
}

func (Body) CsvValue(importCtx ImportCtx) (string, error) {
	body, err := json.Marshal(importCtx.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (Header) CsvValue(importCtx ImportCtx) (string, error) {
	header, err := json.Marshal(importCtx.Header)
	if err != nil {
		return "", err
	}
	return string(header), nil
}

func (Template) CsvValue(_ ImportCtx) (string, error) {
	return "", nil
}

type dummyColumn struct{}

func (dummyColumn) CsvValue(_ ImportCtx) (string, error) {
	return "", nil
}
