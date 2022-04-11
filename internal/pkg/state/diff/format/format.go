// Package format converts diff results to string, see Format function.
package format

import (
	"fmt"
	"reflect"
	"strings"

	diffstr "github.com/kylelemons/godebug/diff"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
)

const MaxEqualLinesInString = 5 // maximum of equal lines returned by strings diff

type PathFormatter interface {
	KeyFsPath(key model.Key) (string, bool)
	ObjectFsPath(object model.Object) (string, bool)
}

type Option func(cfg *formatConfig)

type formatter struct {
	formatConfig
	result  *diff.Result
	builder strings.Builder
}

type formatConfig struct {
	registry     *naming.Registry
	generator    *naming.Generator
	details      bool
	includeEqual bool
}

// Format diff result to string. Process can be modified by options.
func Format(result *diff.Result, options ...Option) string {
	return newFormatter(result, options...).format()
}

func WithNamingRegistry(v *naming.Registry) Option {
	return func(cfg *formatConfig) {
		cfg.registry = v
	}
}

func WithNamingGenerator(v *naming.Generator) Option {
	return func(cfg *formatConfig) {
		cfg.generator = v
	}
}

func WithDetails() Option {
	return func(cfg *formatConfig) {
		cfg.details = true
	}
}

func WithEqualResults() Option {
	return func(cfg *formatConfig) {
		cfg.includeEqual = true
	}
}

func newFormatter(result *diff.Result, options ...Option) *formatter {
	cfg := &formatConfig{}
	for _, option := range options {
		option(cfg)
	}
	return &formatter{result: result, formatConfig: *cfg}
}

func (f *formatter) KeyFsPath(key model.Key) (string, bool) {
	if object, found := f.result.A.Get(key); found {
		return f.ObjectFsPath(object)
	}
	if object, found := f.result.B.Get(key); found {
		return f.ObjectFsPath(object)
	}
	return "", false
}

func (f *formatter) ObjectFsPath(object model.Object) (string, bool) {
	if f.generator != nil {
		if objectPath, err := f.generator.GetOrGenerate(object); err == nil {
			return objectPath.String(), true
		}
	} else if f.registry != nil {
		if objectPath, found := f.registry.PathByKey(object.Key()); found {
			return objectPath.String(), true
		}
	}
	return "", false
}

func (f *formatter) format() string {
	f.builder.Reset()
	for _, result := range f.result.Results {
		// Skip equal
		if !f.includeEqual && result.State == diff.ResultEqual {
			continue
		}

		// First line: <mark> <kind> <path>, eg. "+ C branch/config"
		f.write(mark(result.State))
		f.write(" ")
		f.write(result.Key.Kind().Abbr)
		f.write(" ")
		f.write(f.fsOrLogicPath(result))
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

func (f *formatter) formatDetails(result *diff.ResultObject) {
	// Set prefix for all object lines
	prefix := mark(result.State) + " "
	if result.State == diff.ResultNotEqual {
		// Object is present in both, A and B.
		// Line prefix is empty because individual lines are marked with OnlyInAMark/OnlyInBMark
		prefix = "  "
	}

	// Format each found difference
	for _, item := range result.Differences {
		f.formatItem(result.Key, item, prefix)
	}
}

func (f *formatter) formatItem(parentKey model.Key, item *diff.ResultItem, prefix string) {
	// Set prefix for all item lines
	subPrefix := prefix + mark(item.State) + " "
	if item.State == diff.ResultNotEqual {
		// Item is present in both, A and B.
		// Line prefix is empty because individual lines are marked with OnlyInAMark/OnlyInBMark
		subPrefix = prefix + "  "
	}

	// Write path
	f.write(subPrefix)
	f.write(item.Path.String() + ":")
	f.write("\n")

	// Format value
	f.formatValue(item, subPrefix)
}

func (f *formatter) formatValue(item *diff.ResultItem, prefix string) {
	// Type is included in the result if it differs
	valueA, typeA := diff.CoreType(item.A.Transformed)
	valueB, typeB := diff.CoreType(item.B.Transformed)
	includeType := valueA.IsValid() && valueB.IsValid() && typeA.String() != typeB.String()

	// Process types with defined Format method.
	// Scan all intermediate transforms to find a Formattable value.
	if lastStep := item.Path.Last(); lastStep != nil {
		for _, v := range item.Path.Last().Transforms() {
			fmtA, _ := v.A.(Formattable)
			fmtB, _ := v.B.(Formattable)
			if fmtA != nil || fmtB != nil {
				if valueA.IsValid() {
					valueA, typeA = diff.CoreType(reflect.ValueOf(fmtA.Format(f)))
				}
				if valueB.IsValid() {
					valueB, typeB = diff.CoreType(reflect.ValueOf(fmtB.Format(f)))
				}
			}
		}
	}

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

	if v, ok := value.Interface().(Formattable); ok {
		formatted = v.Format(f)
	} else {
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
				f.write(prefix)
				f.write("  ...\n")
				break
			}

			if len(line) > 0 {
				f.write(prefix)
				f.write("  ")
				f.write(line)
			}
			f.write("\n")
		}
	}
}

func (f *formatter) fsOrLogicPath(result *diff.ResultObject) string {
	// Get object path
	if path, found := f.ObjectFsPath(result.AOrBObject()); found {
		return path
	}

	// Fallback
	return result.Key.LogicPath()
}
