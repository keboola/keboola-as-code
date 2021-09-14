package utils

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/iancoleman/orderedmap"

	"keboola-as-code/src/json"
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

func ReadTaggedFields(dir, relPath, tag string, target interface{}, errPrefix string) error {
	// Read fields with metaFile tag
	metaFields := GetFieldsWithTag(tag, target)
	if len(metaFields) == 0 {
		return nil
	}

	// Read file content
	content := make(map[string]interface{})
	if err := json.ReadFile(dir, relPath, &content, errPrefix); err != nil {
		return err
	}

	// Set values
	reflection := reflect.ValueOf(target).Elem()
	for _, field := range metaFields {
		// Set value, some value are optional, model will be validated later
		if value, ok := content[field.JsonName()]; ok {
			reflection.FieldByName(field.Name).Set(reflect.ValueOf(value))
		}
	}

	return nil
}
