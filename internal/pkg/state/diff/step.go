package diff

import (
	"fmt"
	"reflect"

	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// Step is one item from the Path.
type Step interface {
	A() ResultValue           // value from the A collection
	B() ResultValue           // value from the B collection
	Type() reflect.Type       // type of the values
	IsHidden() bool           // step is not part of the output string
	Transforms() []ValuesPair // intermediate values from all applied cmp.Transform
	AddTransform(transform cmp.Transform)
	String() string
}

// step is common part of the Step interface.
type step struct {
	a          ResultValue
	b          ResultValue
	t          reflect.Type
	hidden     bool
	transforms []cmp.Transform
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

func newStepKind(kind model.Kind, cmpPath cmp.PathStep) *StepKind {
	s := &StepKind{}
	s.Kind = kind
	s.setValues(cmpPath)
	return s
}

func newStepObject(key model.Key, cmpPath cmp.PathStep, hidden bool) *StepObject {
	s := &StepObject{}
	s.Key = key
	s.setValues(cmpPath)
	s.setHidden(hidden)
	return s
}

func newStepStructField(field string, cmpPath cmp.PathStep) *StepStructField {
	s := &StepStructField{}
	s.Field = field
	s.setValues(cmpPath)
	return s
}

func newStepSliceIndex(index uint, cmpPath cmp.PathStep) *StepSliceIndex {
	s := &StepSliceIndex{}
	s.Index = index
	s.setValues(cmpPath)
	return s
}

func newStepMapIndex(index interface{}, cmpPath cmp.PathStep) *StepMapIndex {
	s := &StepMapIndex{}
	s.Index = index
	s.setValues(cmpPath)
	return s
}

func (s step) A() ResultValue {
	return s.a
}

func (s step) B() ResultValue {
	return s.b
}

func (s step) Type() reflect.Type {
	return s.t
}

func (s step) IsHidden() bool {
	return s.hidden
}

func (s *step) Transforms() (out []ValuesPair) {
	var a, b interface{}

	// Add original values
	if s.a.Original.IsValid() {
		a = s.a.Original.Interface()
	}
	if s.b.Original.IsValid() {
		b = s.b.Original.Interface()
	}
	out = append(out, ValuesPair{A: a, B: b})

	// Add all intermediate values from transforms
	for _, t := range s.transforms {
		aRef, bRef := t.Values()
		if aRef.IsValid() {
			a = aRef.Interface()
		}
		if bRef.IsValid() {
			b = bRef.Interface()
		}
		out = append(out, ValuesPair{A: a, B: b})
	}
	return out
}

func (s *step) AddTransform(v cmp.Transform) {
	s.transforms = append(s.transforms, v)
	s.a.Transformed, s.b.Transformed = v.Values()
}

func (s *step) setValues(step cmp.PathStep) {
	a, b := step.Values()
	s.a = NewResultValue(a)
	s.b = NewResultValue(b)
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
	if s.a.Original.IsValid() && !s.a.Original.IsNil() {
		return s.a.Original.Interface().(*model.ObjectNode).Object
	}
	return s.b.Original.Interface().(*model.ObjectNode).Object
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
