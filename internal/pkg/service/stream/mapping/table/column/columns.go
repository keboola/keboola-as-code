package column

import (
	"encoding/json"
	"reflect"
)

type Columns []Column

func (v Columns) Names() (out []string) {
	for _, col := range v {
		out = append(out, col.ColumnName())
	}
	return out
}

func (v Columns) PrimaryKey() []string {
	pk := make([]string, 0)
	for _, c := range v {
		if c.IsPrimaryKey() {
			pk = append(pk, c.ColumnName())
		}
	}
	return pk
}

func (v Columns) MarshalJSON() ([]byte, error) {
	var items []json.RawMessage

	for _, column := range v {
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
			Name       string `json:"name"`
			Type       Type   `json:"type"`
			PrimaryKey bool   `json:"primaryKey"`
		}{}
		if err := json.Unmarshal(item, &t); err != nil {
			return err
		}

		data, err := MakeColumn(t.Type, t.Name, t.PrimaryKey)
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
