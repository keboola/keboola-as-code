package column

import (
	"encoding/json"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/go-jsonnet/ast"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// To add a new column type `Foo`:
// - create a struct for it: `type Foo struct{}`
// - create a type name constant for it: `columnIDFoo = "foo"`
// - add it to `columnToType` and `typeToColumn`
// - implement `Column` for `ColumnFoo`

type Columns []Column

type (
	ID struct {
		Name string `json:"name" validate:"required"`
	}
	Datetime struct {
		Name string `json:"name" validate:"required"`
	}
	IP struct {
		Name string `json:"name" validate:"required"`
	}
	Body struct {
		Name string `json:"name" validate:"required"`
	}
	Headers struct {
		Name string `json:"name" validate:"required"`
	}
)

const (
	IDPlaceholder               = "<<~~id~~>>"
	TemplateLanguageJsonnet     = "jsonnet"
	UndefinedValueStrategyNull  = "null"
	UndefinedValueStrategyError = "error"
)

type Template struct {
	Name                   string `json:"name" validate:"required"`
	Language               string `json:"language" validate:"required,oneof=jsonnet"`
	UndefinedValueStrategy string `json:"undefinedValueStrategy" validate:"required,oneof=null error"`
	Content                string `json:"content" validate:"required,min=1,max=4096"`
}

const (
	columnIDType       = "id"
	columnDatetimeType = "datetime"
	columnIPType       = "ip"
	columnBodyType     = "body"
	columnHeadersType  = "headers"
	columnTemplateType = "template"
)

func (v Columns) Names() (out []string) {
	for _, col := range v {
		out = append(out, col.ColumnName())
	}
	return out
}

func (v Columns) MarshalJSON() ([]byte, error) {
	var items []json.RawMessage

	for _, column := range v {
		column := column

		typ := column.ColumnType()

		typeJSON, err := json.Marshal(typ)
		if err != nil {
			return nil, err
		}

		columnJSON, err := json.Marshal(&column)
		if err != nil {
			return nil, err
		}
		columnJSON = columnJSON[1 : len(columnJSON)-1]

		item := json.RawMessage(`{"type":`)
		item = append(item, typeJSON...)
		if len(columnJSON) > 0 {
			item = append(item, byte(','))
			item = append(item, columnJSON...)
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
			Name string `json:"name"`
			Type string `json:"type"`
		}{}
		if err := json.Unmarshal(item, &t); err != nil {
			return err
		}

		data, err := MakeColumn(t.Type, t.Name)
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

// MakeColumn returns the stringified type of the column.
//
// This function returns `column` as a value.
func MakeColumn(typ string, name string) (Column, error) {
	switch typ {
	case columnIDType:
		return ID{Name: name}, nil
	case columnDatetimeType:
		return Datetime{Name: name}, nil
	case columnIPType:
		return IP{Name: name}, nil
	case columnBodyType:
		return Body{Name: name}, nil
	case columnHeadersType:
		return Headers{Name: name}, nil
	case columnTemplateType:
		return Template{Name: name}, nil
	default:
		return nil, errors.Errorf(`invalid column type name "%s"`, typ)
	}
}

type ImportCtx struct {
	Body     *orderedmap.OrderedMap
	DateTime time.Time
	Headers  *orderedmap.OrderedMap
	IP       net.IP
}

func NewImportCtx(body *orderedmap.OrderedMap, header http.Header, ip net.IP) ImportCtx {
	headers := orderedmap.New()
	for k := range header {
		headers.Set(k, header.Get(k))
	}
	headers.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})

	return ImportCtx{
		Body:     body,
		DateTime: time.Now(),
		Headers:  headers,
		IP:       ip,
	}
}

// Column is an interface used to restrict valid column types.
type Column interface {
	ColumnName() string
	ColumnType() string
	CsvValue(importCtx ImportCtx) (string, error)
}

func (c ID) ColumnName() string       { return c.Name }
func (c Datetime) ColumnName() string { return c.Name }
func (c IP) ColumnName() string       { return c.Name }
func (c Body) ColumnName() string     { return c.Name }
func (c Headers) ColumnName() string  { return c.Name }
func (c Template) ColumnName() string { return c.Name }

func (c ID) ColumnType() string       { return columnIDType }
func (c Datetime) ColumnType() string { return columnDatetimeType }
func (c IP) ColumnType() string       { return columnIPType }
func (c Body) ColumnType() string     { return columnBodyType }
func (c Headers) ColumnType() string  { return columnHeadersType }
func (c Template) ColumnType() string { return columnTemplateType }

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

func (Headers) CsvValue(importCtx ImportCtx) (string, error) {
	header, err := json.Marshal(importCtx.Headers)
	if err != nil {
		return "", err
	}
	return string(header), nil
}

func (c Template) CsvValue(importCtx ImportCtx) (string, error) {
	if c.Language == TemplateLanguageJsonnet {
		ctx := jsonnet.NewContext()
		ctx.NativeFunctionWithAlias(getBodyPath(c, importCtx.Body))
		ctx.NativeFunctionWithAlias(getBody(importCtx.Body))
		ctx.NativeFunctionWithAlias(getHeader(c, importCtx.Headers))
		ctx.NativeFunctionWithAlias(getHeaders(importCtx.Headers))
		ctx.GlobalBinding("currentDatetime", jsonnet.ValueToLiteral(importCtx.DateTime.Format(time.RFC3339)))

		res, err := jsonnet.Evaluate(c.Content, ctx)
		if err != nil {
			return "", err
		}
		return strings.TrimRight(res, "\n"), nil
	}
	return "", errors.Errorf(`unsupported language "%s", use jsonnet instead`, c.Language)
}

func getBodyPath(t Template, om *orderedmap.OrderedMap) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   "BodyPath",
		Params: ast.Identifiers{"path"},
		Func: func(params []interface{}) (any, error) {
			if len(params) > 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			} else if path, ok := params[0].(string); !ok {
				return nil, errors.New("parameter must be a string")
			} else {
				val, found, err := om.GetNested(path)
				if !found {
					if t.UndefinedValueStrategy == UndefinedValueStrategyNull {
						return nil, nil
					} else {
						return nil, errors.Errorf(`path "%s" not found in the body'`, path)
					}
				}
				if err != nil {
					return nil, err
				}

				return valueToJSONType(val), nil
			}
		},
	}
}

func getBody(om *orderedmap.OrderedMap) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name: "Body",
		Func: func(params []interface{}) (any, error) {
			return valueToJSONType(om), nil
		},
	}
}

func valueToJSONType(in any) any {
	if v, ok := in.(*orderedmap.OrderedMap); ok {
		m := make(map[string]any)
		for _, k := range v.Keys() {
			m[k], _ = v.Get(k)
			m[k] = valueToJSONType(m[k])
		}
		return m
	}
	if v, ok := in.([]any); ok {
		for i, arrVal := range v {
			v[i] = valueToJSONType(arrVal)
		}
		return v
	}

	return in
}

func getHeader(t Template, om *orderedmap.OrderedMap) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   "Header",
		Params: ast.Identifiers{"path"},
		Func: func(params []interface{}) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			} else if key, ok := params[0].(string); !ok {
				return nil, errors.New("parameter must be a string")
			} else {
				val, found := om.Get(http.CanonicalHeaderKey(key))
				if !found {
					if t.UndefinedValueStrategy == UndefinedValueStrategyNull {
						return nil, nil
					} else {
						return nil, errors.Errorf(`header "%s" not found'`, http.CanonicalHeaderKey(key))
					}
				}
				return val, nil
			}
		},
	}
}

func getHeaders(om *orderedmap.OrderedMap) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name: "Headers",
		Func: func(params []interface{}) (any, error) {
			return valueToJSONType(om), nil
		},
	}
}
