package model

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ImportCondition struct {
	Count int               `json:"count" validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" validate:"min=100,max=50000000"`
	Time  time.Duration     `json:"time" validate:"min=30s,max=24h"`
}

type Export struct {
	ID               string            `json:"exportId" validate:"required,min=1,max=48"`
	Name             string            `json:"name" validate:"required,min=1,max=40"`
	ImportConditions []ImportCondition `json:"importConditions" validate:"required"`
}

type TableID struct {
	Stage      string `json:"stage"`
	BucketName string `json:"bucketName"`
	TableName  string `json:"tableName"`
}

func (t TableID) String() string {
	return fmt.Sprintf("%s.c-%s.%s", t.Stage, t.BucketName, t.TableName)
}

type Mapping struct {
	RevisionID  int     `json:"revisionId" validate:"required"`
	TableID     TableID `json:"tableId" validate:"required,min=1,max=198"`
	Incremental bool    `json:"incremental" validate:"required"`
	Columns     Columns `json:"columns" validate:"required,min=1,max=50"`
}

type Receiver struct {
	ID        string `json:"receiverId" validate:"required,min=1,max=48"`
	ProjectID int    `json:"projectId" validate:"required"`
	Name      string `json:"name" validate:"required,min=1,max=40"`
	Secret    string `json:"secret" validate:"required,len=48"`
}

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
	Headers  struct{}
)

const (
	UndefinedValueStrategyNull  = "null"
	UndefinedValueStrategyError = "error"
)

type ColumnTemplate struct {
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
	items := []byte("[")

	for i, column := range v {
		column := column

		typ, err := columnToType(column)
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

		item := []byte(`{"type":`)
		item = append(item, typeJson...)
		if len(columnJson) > 0 {
			item = append(item, byte(','))
			item = append(item, columnJson...)
		}
		item = append(item, byte('}'))

		items = append(items, item...)
		if i != len(v)-1 {
			items = append(items, byte(','))
		}
	}
	items = append(items, ']')

	return items, nil
}

func (v *Columns) UnmarshalJSON(b []byte) error {
	*v = nil

	var items []map[string]any
	if err := json.Unmarshal(b, &items); err != nil {
		return err
	}

	for _, item := range items {
		itemBytes, _ := json.Marshal(item)

		t := struct {
			Type string `json:"type"`
		}{}
		if err := json.Unmarshal(itemBytes, &t); err != nil {
			return err
		}

		data, err := typeToColumn(t.Type)
		if err != nil {
			return err
		}

		ptr := reflect.New(reflect.TypeOf(data))
		ptr.Elem().Set(reflect.ValueOf(data))

		if err = json.Unmarshal(itemBytes, ptr.Interface()); err != nil {
			return err
		}

		*v = append(*v, ptr.Elem().Interface().(Column))
	}

	return nil
}

// columnToType returns the stringified type of the column.
//
// This function expects `column` to be passed by value.
func columnToType(column any) (string, error) {
	switch column.(type) {
	case ID:
		return columnIDType, nil
	case Datetime:
		return columnDatetimeType, nil
	case IP:
		return columnIPType, nil
	case Body:
		return columnBodyType, nil
	case Headers:
		return columnHeadersType, nil
	case ColumnTemplate:
		return columnTemplateType, nil
	default:
		return "", errors.Errorf(`invalid column type "%T"`, column)
	}
}

// ColumnToType returns the stringified type of the column.
//
// This function returns `column` as a value.
func typeToColumn(typ string) (Column, error) {
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
		return Headers{}, nil
	case columnTemplateType:
		return ColumnTemplate{}, nil
	default:
		return dummyColumn{}, errors.Errorf(`invalid column type name "%s"`, typ)
	}
}

// Column is an interface used to restrict valid column types.
type Column interface {
	IsColumn() bool
}

func (ID) IsColumn() bool             { return true }
func (Datetime) IsColumn() bool       { return true }
func (IP) IsColumn() bool             { return true }
func (Body) IsColumn() bool           { return true }
func (Headers) IsColumn() bool        { return true }
func (ColumnTemplate) IsColumn() bool { return true }

type dummyColumn struct{}

func (dummyColumn) IsColumn() bool { return true }
