package utils

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/iancoleman/orderedmap"
)

type StructField struct {
	jsonName string
	reflect.StructField
}

func (f *StructField) JsonName() string {
	if f.jsonName != "" {
		return f.jsonName
	}
	return f.StructField.Name
}

func MapFromTaggedFields(tag string, model interface{}) *orderedmap.OrderedMap {
	fields := GetFieldsWithTag(tag, model)
	if len(fields) == 0 {
		return nil
	}

	target := orderedmap.New()
	reflection := reflect.ValueOf(model).Elem()
	for _, field := range fields {
		target.Set(field.JsonName(), reflection.FieldByName(field.Name).Interface())
	}
	return target
}

func MapFromOneTaggedField(tag string, model interface{}) *orderedmap.OrderedMap {
	field := GetOneFieldWithTag(tag, model)
	if field == nil {
		return nil
	}

	reflection := reflect.ValueOf(model).Elem()
	return reflection.FieldByName(field.Name).Interface().(*orderedmap.OrderedMap)
}

func StringFromOneTaggedField(tag string, model interface{}) (str string, found bool) {
	field := GetOneFieldWithTag(tag, model)
	if field == nil {
		return "", false
	}
	if field.Type.String() != "string" {
		return "", false
	}

	reflection := reflect.ValueOf(model).Elem()
	return reflection.FieldByName(field.Name).Interface().(string), true
}

func GetFieldsWithTag(tag string, model interface{}) []*StructField {
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
	for i := 0; i < numFields; i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get(tagName)
		if tag == tagValue {
			jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
			fields = append(fields, &StructField{jsonName: jsonName, StructField: field})
		}
	}
	return fields
}

func GetOneFieldWithTag(tag string, model interface{}) *StructField {
	fields := GetFieldsWithTag(tag, model)
	if len(fields) > 1 {
		panic(fmt.Errorf("struct \"%T\" has multiple fields with tag `%s`, but only one allowed", model, tag))
	}

	if len(fields) == 1 {
		return fields[0]
	}

	return nil
}

func SetFields(fields []*StructField, data *orderedmap.OrderedMap, target interface{}) {
	reflection := reflect.ValueOf(target).Elem()
	for _, field := range fields {
		// Set value, some values are optional, model will be validated later
		if value, ok := data.Get(field.JsonName()); ok {
			reflection.FieldByName(field.Name).Set(reflect.ValueOf(value))
		}
	}
}

func SetField(field *StructField, value, target interface{}) {
	reflection := reflect.ValueOf(target).Elem()
	reflection.FieldByName(field.Name).Set(reflect.ValueOf(value))
}
