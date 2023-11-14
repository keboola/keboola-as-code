package configmap

import (
	"reflect"
)

type ValueContainer interface {
	ValueType() reflect.Type
	TrySetValue(value reflect.Value) bool
}

type withOrigin interface {
	SetOrigin(v SetBy)
}

// fieldValue is an auxiliary structure, it stores value origin.
type fieldValue struct {
	Value any
	SetBy SetBy
}

// Value implements the ValueContainer and withOrigin interfaces.
// In addition to the value, it is also possible to find out how it was set.
type Value[T any] struct {
	Value T `configKey:",squash"`
	SetBy SetBy
}

func (v fieldValue) IsSet() bool {
	return v.SetBy != SetByDefault
}

func (v *Value[T]) IsSet() bool {
	return v.SetBy != SetByDefault
}

func (v *Value[T]) ValueType() reflect.Type {
	return reflect.TypeOf(v.Value)
}

func (v *Value[T]) TrySetValue(value reflect.Value) bool {
	if t := v.ValueType(); value.CanConvert(t) {
		v.Value = value.Convert(t).Interface().(T)
		return true
	} else {
		return false
	}
}

func (v *Value[T]) SetOrigin(value SetBy) {
	v.SetBy = value
}
