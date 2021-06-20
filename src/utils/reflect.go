package utils

import "reflect"

func GetFieldsWithTag(tag, value string, modelType reflect.Type, model interface{}) []reflect.StructField {
	var fields []reflect.StructField
	numFields := modelType.NumField()
	for i := 0; i < numFields; i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get(tag)
		if tag == value {
			fields = append(fields, field)
		}
	}
	return fields
}
