package diff

import (
	"fmt"
	"reflect"
	"strings"

	diffstr "github.com/kylelemons/godebug/diff"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
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

type FormatOption func(cfg *formatConfig)

type formatter struct {
	formatConfig
	builder strings.Builder
}

type formatConfig struct {
	pathFormatter pathFormatter
	details       bool
	includeEqual  bool
}

func WithNamingRegistry(v *naming.Registry) FormatOption {
	return func(cfg *formatConfig) {
		cfg.pathFormatter.registry = v
	}
}

func WithNamingGenerator(v *naming.Generator) FormatOption {
	return func(cfg *formatConfig) {
		cfg.pathFormatter.generator = v
	}
}

func WithDetails() FormatOption {
	return func(cfg *formatConfig) {
		cfg.details = true
	}
}

func WithEqualResults() FormatOption {
	return func(cfg *formatConfig) {
		cfg.includeEqual = true
	}
}

func format(r *Result, options ...FormatOption) string {
	return newFormatter(options...).format(r)
}

func newFormatter(options ...FormatOption) *formatter {
	cfg := &formatConfig{}
	for _, option := range options {
		option(cfg)
	}
	return &formatter{formatConfig: *cfg}
}

func (f *formatter) format(r *Result) string {
	f.builder.Reset()
	for _, result := range r.Results {
		// Skip equal
		if !f.includeEqual && result.State == ResultEqual {
			continue
		}

		// First line: <mark> <kind> <path>, eg. "+ C branch/config"
		f.write(result.State.Mark())
		f.write(" ")
		f.write(result.Key.Kind().Abbr)
		f.write(" ")
		f.write(f.pathFormatter.FormatObjectPath(result))
		if !f.details && !result.Differences.IsEmpty() {
			f.write(" | changes: ")
			f.write(result.Differences.ShortString())
		}
		f.write("\n")

		// Format details
		if f.details {
			f.formatDetails(result)
		}
	}
	return f.builder.String()
}

func (f *formatter) write(s string) {
	_, _ = f.builder.WriteString(s)
}

func (f *formatter) formatDetails(result *ResultObject) {
	// Set prefix for all object lines
	prefix := result.State.Mark() + " "
	if result.State == ResultNotEqual {
		// Object is present in both, A and B.
		// Line prefix is empty because individual lines are marked with OnlyInAMark/OnlyInBMark
		prefix = "  "
	}

	// Format each found difference
	for _, item := range result.Differences {
		f.formatItem(item, prefix)
	}
}

func (f *formatter) formatItem(item *ResultItem, prefix string) {
	// Set prefix for all item lines
	subPrefix := prefix + item.State.Mark() + " "
	if item.State == ResultNotEqual {
		// Item is present in both, A and B.
		// Line prefix is empty because individual lines are marked with OnlyInAMark/OnlyInBMark
		subPrefix = prefix + "  "
	}

	// Write path
	f.write(subPrefix)
	f.write(f.pathFormatter.FormatValuePath(item))
	f.write("\n")

	// Format value
	f.formatValue(item, subPrefix)
}

func (f *formatter) formatValue(item *ResultItem, prefix string) {
	// Type is included in the result if it differs
	valueA, typeA := coreType(item.A)
	valueB, typeB := coreType(item.B)
	includeType := valueA.IsValid() && valueB.IsValid() && typeA.String() != typeB.String()

	// Format strings
	if valueA.IsValid() && valueB.IsValid() && typeA.String() == "string" && typeB.String() == "string" {
		f.formatStrings(valueA.String(), valueB.String(), prefix)
		return
	}

	// Format other types
	if valueA.IsValid() {
		subPrefix := prefix
		if valueB.IsValid() {
			subPrefix += OnlyInAMark + " "
		} else {
			subPrefix += "  "
		}
		f.formatValueSide(valueA, typeA, includeType, subPrefix)
	}
	if valueB.IsValid() {
		subPrefix := prefix
		if valueA.IsValid() {
			subPrefix += OnlyInBMark + " "
		} else {
			subPrefix += "  "
		}
		f.formatValueSide(valueB, typeB, includeType, subPrefix)
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

type pathFormatter struct {
	registry  *naming.Registry
	generator *naming.Generator
}

func (f pathFormatter) FormatObjectPath(v *ResultObject) string {
	if f.generator != nil {
		if objectPath, err := f.generator.GetOrGenerate(v.AOrBObject()); err == nil {
			return objectPath.String()
		}
	} else if f.registry != nil {
		if objectPath, found := f.registry.PathByKey(v.Key); found {
			return objectPath.String()
		}
	}
	return v.Key.LogicPath()
}

func (f pathFormatter) FormatValuePath(v *ResultItem) string {
	if objectStep, ok := v.Path.Last().(StepObject); ok {
		if f.generator != nil {
			if objectPath, err := f.generator.GetOrGenerate(objectStep.AOrBObject()); err == nil {
				return objectPath.String()
			}
		} else if f.registry != nil {
			if objectPath, found := f.registry.PathByKey(objectStep.Key); found {
				return objectPath.String()
			}
		}
	}
	return v.Path.String()
}
