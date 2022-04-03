package diff

import (
	"reflect"
	"strings"
)

// Marks representing the result of the diff.
const (
	EqualMark    = "="
	NotEqualMark = "*"
	AddMark      = "+"
	DeleteMark   = "×"
	OnlyInAMark  = "-"
	OnlyInBMark  = "+"
)

type FormatOptions struct {
	Details      bool
	IncludeEqual bool
}

type formatter struct {
	FormatOptions
	builder strings.Builder
}

func (f *formatter) write(s string) {
	_, _ = f.builder.WriteString(s)
}

func format(r *Result, o FormatOptions) string {
	f := &formatter{FormatOptions: o}
	f.format(r)
	return f.builder.String()
}

func (f *formatter) format(r *Result) {
	for _, result := range r.Results {
		// Skip equal
		if !f.IncludeEqual && result.State == ResultEqual {
			continue
		}

		// Get object filesystem or logic path
		var path string
		if result.FsPath == nil {
			path = result.Key.LogicPath()
		} else {
			path = result.FsPath.String()
		}

		// First line: <mark> <kind> <path>, eg. "+ C branch/config"
		f.write(result.State.Mark())
		f.write(" ")
		f.write(result.Key.Kind().Abbr)
		f.write(" ")
		f.write(path)
		if !f.Details && !result.Differences.IsEmpty() {
			f.write(" | changes: ")
			f.write(result.Differences.ShortString())
		}
		f.write("\n")

		// Format details
		if f.Details {
			f.formatDetails(result)
		}
	}
}

func (f *formatter) formatDetails(result *ResultObject) {
	// Set prefix for all object lines
	objectMark := result.State.Mark() + " "
	if result.State == ResultNotEqual {
		// Value is present in both, A and B.
		// Line prefix is empty because individual lines are marked with OnlyInAMark/OnlyInBMark
		objectMark = "  "
	}

	// Format each found difference
	for _, value := range result.Differences {
		f.formatValue(value, objectMark)
	}
}

func (f *formatter) formatValue(value *ResultValue, objectMark string) {
	// Type is included in the result if it differs
	valueA, typeA := coreType(value.A)
	valueB, typeB := coreType(value.B)
	includeType := valueA.IsValid() && valueB.IsValid() && !valueA.IsZero() && !valueB.IsZero() && typeA.String() != typeB.String()

	// Format value sides
	if valueA.IsValid() {
		valueMark := ``
		if valueB.IsValid() {
			valueMark = OnlyInAMark + " "
		}
		f.formatValueSide(valueA, typeA, includeType, objectMark, valueMark)
	}
	if valueB.IsValid() {
		valueMark := ``
		if valueA.IsValid() {
			valueMark = OnlyInBMark + " "
		}
		f.formatValueSide(valueB, typeB, includeType, objectMark, valueMark)
	}
}

func (f *formatter) formatValueSide(value reflect.Value, t reflect.Type, includeType bool, objectMark, valueMark string) []string {

}
