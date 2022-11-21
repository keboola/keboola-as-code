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
	RevisionID  int           `json:"revisionId" validate:"required"`
	TableID     TableID       `json:"tableId" validate:"required,min=1,max=198"`
	Incremental bool          `json:"incremental" validate:"required"`
	Columns     MappedColumns `json:"columns" validate:"required,min=1,max=50"`
}

type Receiver struct {
	ID        string `json:"receiverId" validate:"required,min=1,max=48"`
	ProjectID int    `json:"projectId" validate:"required"`
	Name      string `json:"name" validate:"required,min=1,max=40"`
	Secret    string `json:"secret" validate:"required,len=48"`
}

// To add a new column type `Foo`:
// - create a struct for it: `type ColumnFoo struct{}`
// - create a type name constant for it: `columnIDFoo = "foo"`
// - add it to `columnToType` and `typeToColumn`
// - implement `MappedColumn` for `ColumnFoo`

type MappedColumns []MappedColumn

type (
	ColumnID       struct{}
	ColumnDatetime struct{}
	ColumnIP       struct{}
	ColumnBody     struct{}
	ColumnHeaders  struct{}
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

func (v MappedColumns) MarshalJSON() ([]byte, error) {
	var items [][]byte

	for _, column := range v {
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

		items = append(items, item)
	}

	return json.Marshal(items)
}

func (v *MappedColumns) UnmarshalJSON(b []byte) error {
	*v = nil

	var items [][]byte
	if err := json.Unmarshal(b, &items); err != nil {
		return err
	}

	type TempColumnType struct {
		Type string `json:"type"`
	}

	for _, item := range items {
		var temp TempColumnType
		if err := json.Unmarshal(item, &temp); err != nil {
			return err
		}

		data, err := typeToColumn(temp.Type)
		if err != nil {
			return err
		}

		ptr := reflect.New(reflect.TypeOf(data))
		ptr.Elem().Set(reflect.ValueOf(data))

		if err := json.Unmarshal(item, ptr.Interface()); err != nil {
			return err
		}

		*v = append(*v, ptr.Elem().Interface().(MappedColumn))
	}

	return nil
}

// columnToType returns the stringified type of the column.
//
// This function expects `column` to be passed by value.
func columnToType(column any) (string, error) {
	switch column.(type) {
	case ColumnID:
		return columnIDType, nil
	case ColumnDatetime:
		return columnDatetimeType, nil
	case ColumnIP:
		return columnIPType, nil
	case ColumnBody:
		return columnBodyType, nil
	case ColumnHeaders:
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
func typeToColumn(typ string) (MappedColumn, error) {
	switch typ {
	case columnIDType:
		return ColumnID{}, nil
	case columnDatetimeType:
		return ColumnDatetime{}, nil
	case columnIPType:
		return ColumnIP{}, nil
	case columnBodyType:
		return ColumnBody{}, nil
	case columnHeadersType:
		return ColumnHeaders{}, nil
	case columnTemplateType:
		return ColumnTemplate{}, nil
	default:
		return dummyMappedColumn{}, errors.Errorf(`invalid column type name "%s"`, typ)
	}
}

// MappedColumn is an interface used to restrict valid column types.
type MappedColumn interface {
	IsMappedColumn() bool
}

func (ColumnID) IsMappedColumn() bool       { return true }
func (ColumnDatetime) IsMappedColumn() bool { return true }
func (ColumnIP) IsMappedColumn() bool       { return true }
func (ColumnBody) IsMappedColumn() bool     { return true }
func (ColumnHeaders) IsMappedColumn() bool  { return true }
func (ColumnTemplate) IsMappedColumn() bool { return true }

type dummyMappedColumn struct{}

func (dummyMappedColumn) IsMappedColumn() bool { return true }
