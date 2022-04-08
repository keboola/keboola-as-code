package diff

import (
	"fmt"
	"reflect"
	"strings"

	diffstr "github.com/kylelemons/godebug/diff"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
)

const MaxEqualLinesInString = 5 // maximum of equal lines returned by strings diff

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
	// Write path
	f.write(objectMark)
	f.write(value.Path.String())
	f.write("\n")

	// Type is included in the result if it differs
	valueA, typeA := coreType(value.A)
	valueB, typeB := coreType(value.B)
	includeType := valueA.IsValid() && valueB.IsValid() && !valueA.IsZero() && !valueB.IsZero() && typeA.String() != typeB.String()

	// Format strings
	if valueA.IsValid() && valueB.IsValid() && typeA.String() == "string" && typeB.String() == "string" {
		f.formatStrings(valueA.String(), valueB.String(), objectMark)
		return
	}

	// Format other types
	if valueA.IsValid() {
		prefix := objectMark
		if valueB.IsValid() {
			prefix += OnlyInAMark + " "
		}
		f.formatValueSide(valueA, typeA, includeType, prefix)
	}
	if valueB.IsValid() {
		prefix := objectMark
		if valueA.IsValid() {
			prefix += OnlyInBMark + " "
		}
		f.formatValueSide(valueB, typeB, includeType, prefix)
	}
}

func (f *formatter) formatValueSide(value reflect.Value, t reflect.Type, includeType bool, prefix string) {
	var formatted string
	switch {
	case t.Kind() == reflect.Ptr && value.IsNil():
		formatted = `(null)`
	case t.Kind() == reflect.Map:
		// Format map to JSON
		formatted = strings.TrimRight(json.MustEncodeString(value.Interface(), true), "\n")
	case includeType:
		formatted = fmt.Sprintf(`%#v`, value)
	default:
		formatted = fmt.Sprintf(`%+v`, value)
	}

	// Write lines with prefix
	for _, line := range strings.Split(formatted, "\n") {
		f.write(prefix)
		f.write(line)
		f.write("\n")
	}
}

func (f *formatter) formatStrings(a, b string, prefix string) {
	aLines := strings.Split(a, "\n")
	if len(a) == 0 {
		aLines = []string{}
	}

	bLines := strings.Split(b, "\n")
	if len(b) == 0 {
		bLines = []string{}
	}

	chunks := diffstr.DiffChunks(aLines, bLines)
	for _, c := range chunks {
		for _, line := range c.Added {
			f.write(prefix)
			f.write(OnlyInBMark)
			f.write(" ")
			f.write(line)
			f.write("\n")
		}
		for _, line := range c.Deleted {
			f.write(prefix)
			f.write(OnlyInAMark)
			f.write(" ")
			f.write(line)
			f.write("\n")
		}
		for i, line := range c.Equal {
			// Limit number of equal lines in row
			if i+1 >= MaxEqualLinesInString && len(c.Equal) > MaxEqualLinesInString {
				f.write("  ...\n")
				break
			}

			if len(line) > 0 {
				f.write("  ")
				f.write(line)
			}
			f.write("\n")
		}
	}
}
