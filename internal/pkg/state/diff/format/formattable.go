package format

import (
	"fmt"
	"strings"
)

type Formattable interface {
	Format(f Formatter) string
}

type Placeholders map[InternalValue]ReplaceFn

type InternalValue string

type ReplaceFn func(f Formatter) string

type ValueWithPlaceholders struct {
	Value        string `diff:"true"`
	Placeholders Placeholders
}

func NewPlaceholders() Placeholders {
	return make(Placeholders)
}

func NewPlaceholder(value string) InternalValue {
	return InternalValue(fmt.Sprintf("<<~~placeholder:%s~~>>", value))
}

func (p Placeholders) Add(internalValue InternalValue, fn ReplaceFn) {
	p[internalValue] = fn
}

func (p Placeholders) Replace(str string, f Formatter) string {
	for k, v := range p {
		strings.ReplaceAll(str, string(k), v(f))
	}
	return str
}

func (v ValueWithPlaceholders) Transform() interface{} {
	return v.Value
}

func (v ValueWithPlaceholders) Format(f Formatter) string {
	return v.Placeholders.Replace(v.Value, f)
}
