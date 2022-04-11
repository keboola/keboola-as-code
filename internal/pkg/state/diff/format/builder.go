package format

import (
	"fmt"
	"strings"
)

// Builder is string-like builder with placeholders support.
// It is used to generate diff for those types that we compare as a string.
// For diff generation purposes, default values are used instead of placeholders to determine if the values are the same.
// For diff formatting purposes, placeholders are replaced using the ReplaceFn function.
//
// Example:
// In diff generation, Transform() method returns "branch:123/config:456/row:789" value, and it is compared to the other side.
// In diff formatting, Format(f PathFormatter) method returns "my-branch/my-config/my-row".
// So instead of the logical path to the object, the path in the filesystem is used.
// It does not affect the diff logic, only the formatting.
type Builder struct {
	parts    []interface{}
	finalize FinalizeFn
}

type ReplaceFn func(f PathFormatter) string

type FinalizeFn func(str string) string

type placeholder struct {
	defaultValue string
	replaceFn    ReplaceFn
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Reset() {
	b.parts = nil
}

func (b *Builder) WriteString(str string) {
	b.parts = append(b.parts, str)
}

func (b *Builder) WritePlaceholder(defaultValue string, replaceFn ReplaceFn) {
	b.parts = append(b.parts, placeholder{defaultValue: defaultValue, replaceFn: replaceFn})
}

func (b *Builder) FinalizeFn(fn FinalizeFn) {
	b.finalize = fn
}

// Transform replaces placeholders with default values, output is then compared by diff.
func (b *Builder) Transform() interface{} {
	var out strings.Builder
	for _, part := range b.parts {
		switch v := part.(type) {
		case string:
			out.WriteString(v)
		case placeholder:
			out.WriteString(v.defaultValue) // <<<<<<<<<<
		default:
			panic(fmt.Errorf("unexpected type %T", part))
		}
	}

	str := out.String()
	if b.finalize != nil {
		str = b.finalize(str)
	}
	return str
}

// Format replaces placeholders using ReplaceFn, output is then added to diff string output.
func (b *Builder) Format(f PathFormatter) string {
	var out strings.Builder
	for _, part := range b.parts {
		switch v := part.(type) {
		case string:
			out.WriteString(v)
		case placeholder:
			out.WriteString(v.replaceFn(f)) // <<<<<<<<<<
		default:
			panic(fmt.Errorf("unexpected type %T", part))
		}
	}
	str := out.String()
	if b.finalize != nil {
		str = b.finalize(str)
	}
	return str
}
