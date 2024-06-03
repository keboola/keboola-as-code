package reflecthelper

import (
	"reflect"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type StructField struct {
	jsonName string
	reflect.StructField
}

func (f *StructField) JSONName() string {
	if f.jsonName != "" && f.jsonName != "-" {
		return f.jsonName
	}
	return f.StructField.Name
}

func MapFromTaggedFields(tag string, model any) *orderedmap.OrderedMap {
	fields := GetFieldsWithTag(tag, model)
	if len(fields) == 0 {
		return nil
	}

	target := orderedmap.New()
	reflection := unwrap(reflect.ValueOf(model))

	for _, field := range fields {
		target.Set(field.JSONName(), reflection.FieldByName(field.Name).Interface())
	}
	return target
}

func MapFromOneTaggedField(tag string, model any) *orderedmap.OrderedMap {
	field := GetOneFieldWithTag(tag, model)
	if field == nil {
		return nil
	}
	reflection := unwrap(reflect.ValueOf(model))
	m := reflection.FieldByName(field.Name).Interface().(*orderedmap.OrderedMap)
	return m.Clone()
}

func StringFromOneTaggedField(tag string, model any) (str string, found bool) {
	field := GetOneFieldWithTag(tag, model)
	if field == nil {
		return "", false
	}
	if field.Type.String() != "string" {
		return "", false
	}

	reflection := unwrap(reflect.ValueOf(model))
	return reflection.FieldByName(field.Name).Interface().(string), true
}

func GetFieldsWithTag(tag string, model any) []*StructField {
	parts := strings.SplitN(tag, ":", 2)
	tagName, tagValue := parts[0], parts[1]

	var modelType reflect.Type
	if v, ok := model.(reflect.Type); ok {
		modelType = v
	} else {
		modelType = reflect.TypeOf(model).Elem()
	}

	var fields []*StructField
	numFields := modelType.NumField()
	for i := range numFields {
		field := modelType.Field(i)
		tag := field.Tag.Get(tagName)
		if tag == tagValue {
			jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
			fields = append(fields, &StructField{jsonName: jsonName, StructField: field})
		}
	}
	return fields
}

func GetOneFieldWithTag(tag string, model any) *StructField {
	fields := GetFieldsWithTag(tag, model)
	if len(fields) > 1 {
		panic(errors.Errorf("struct \"%T\" has multiple fields with tag `%s`, but only one allowed", model, tag))
	}

	if len(fields) == 1 {
		return fields[0]
	}

	return nil
}

func SetFields(fields []*StructField, data *orderedmap.OrderedMap, target any) {
	reflection := unwrap(reflect.ValueOf(target))
	for _, field := range fields {
		// Set value, some values are optional, model will be validated later
		if value, ok := data.Get(field.JSONName()); ok {
			reflection.FieldByName(field.Name).Set(reflect.ValueOf(value))
		}
	}
}

func SetField(field *StructField, value, target any) {
	reflection := reflect.ValueOf(target).Elem()
	reflection.FieldByName(field.Name).Set(reflect.ValueOf(value))
}

type objectWithName interface {
	ObjectName() string
	String() string
}

// SortByName - in tests are IDs and sort random -> so we must sort by name.
func SortByName(slice any) any {
	// Check slice
	t := reflect.TypeOf(slice)
	if t.Kind() != reflect.Slice {
		panic(errors.Errorf("expected slice, given \"%T\"", slice))
	}

	// Sort by Name, and by String key if names are same
	value := reflect.ValueOf(slice)
	sort.SliceStable(slice, func(i, j int) bool {
		objI := value.Index(i).Interface().(objectWithName)
		objJ := value.Index(j).Interface().(objectWithName)
		// value = {name}_{string key}
		valueI := objI.ObjectName() + `_` + objI.String()
		valueJ := objJ.ObjectName() + `_` + objJ.String()
		return valueI < valueJ
	})

	return slice
}

// unwrap all interfaces and pointers.
func unwrap(v reflect.Value) reflect.Value {
	for {
		if v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr {
			v = v.Elem()
		} else {
			return v
		}
	}
}
