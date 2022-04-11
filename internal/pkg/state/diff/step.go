package diff

import (
	"fmt"
	"reflect"

	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// Step is one item from the Path.
type Step interface {
	A() reflect.Value
	B() reflect.Value
	Type() reflect.Type
	IsHidden() bool
	String() string
}

type step struct {
	a      reflect.Value
	b      reflect.Value
	t      reflect.Type
	hidden bool
}

// StepKind groups Object children of the same Kind.
type StepKind struct {
	step
	Kind model.Kind
}

// StepObject represents a child Object.
type StepObject struct {
	step
	Key model.Key
}

// StepStructField is a struct field.
type StepStructField struct {
	step
	Field string
}

// StepSliceIndex is a slice index.
type StepSliceIndex struct {
	step
	Index uint
}

// StepMapIndex is a map index.
type StepMapIndex struct {
	step
	Index interface{}
}

func newStepKind(kind model.Kind, cmpPath cmp.PathStep) StepKind {
	s := StepKind{}
	s.Kind = kind
	s.setValues(cmpPath)
	return s
}

func newStepObject(key model.Key, cmpPath cmp.PathStep, hidden bool) StepObject {
	s := StepObject{}
	s.Key = key
	s.setValues(cmpPath)
	s.setHidden(hidden)
	return s
}

func newStepStructField(field string, cmpPath cmp.PathStep) StepStructField {
	s := StepStructField{}
	s.Field = field
	s.setValues(cmpPath)
	return s
}

func newStepSliceIndex(index uint, cmpPath cmp.PathStep) StepSliceIndex {
	s := StepSliceIndex{}
	s.Index = index
	s.setValues(cmpPath)
	return s
}

func newStepMapIndex(index interface{}, cmpPath cmp.PathStep) StepMapIndex {
	s := StepMapIndex{}
	s.Index = index
	s.setValues(cmpPath)
	return s
}

func (s step) A() reflect.Value {
	return s.a
}

func (s step) B() reflect.Value {
	return s.b
}

func (s step) Type() reflect.Type {
	return s.t
}

func (s step) IsHidden() bool {
	return s.hidden
}

func (s *step) setValues(step cmp.PathStep) {
	s.a, s.b = step.Values()
	s.t = step.Type()
}

func (s *step) setHidden(v bool) {
	s.hidden = v
}

func (s StepKind) String() string {
	return s.Kind.String()
}

func (s StepObject) String() string {
	return fmt.Sprintf("[%s]", s.Key.ObjectId())
}

func (s StepObject) AOrBObject() model.Object {
	if s.a.IsValid() && !s.a.IsNil() {
		return s.a.Interface().(*model.ObjectNode).Object
	}
	return s.b.Interface().(*model.ObjectNode).Object
}

func (s StepStructField) String() string {
	return s.Field
}

func (s StepSliceIndex) String() string {
	return fmt.Sprintf("[%d]", s.Index)
}

func (s StepMapIndex) String() string {
	return fmt.Sprintf("%s", s.Index)
}
