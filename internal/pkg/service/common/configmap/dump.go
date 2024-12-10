package configmap

import (
	jsonLib "encoding/json"
	"reflect"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type DumpError struct {
	Dump            []byte
	ValidationError error
}

func (h DumpError) Error() string {
	return "configuration dump requested"
}

// Dumper dumps values from one or more configuration structures to JSON or YAML format.
type Dumper struct {
	values *orderedmap.OrderedMap
	errors errors.MultiError
}

// dumpedValue is an auxiliary structure used during dump, HeadComment is used to generate YAML comments.
type dumpedValue struct {
	Value       any
	HeadComment string
}

func NewDumper() *Dumper {
	return &Dumper{values: orderedmap.New(), errors: errors.NewMultiError()}
}

func (v dumpedValue) MarshalJSON() ([]byte, error) {
	// Marshal underlying value
	return jsonLib.Marshal(v.Value)
}

// Dump the configuration structure to an internal buffer.
func (d *Dumper) Dump(v any) *Dumper {
	err := Visit(reflect.ValueOf(v), VisitConfig{
		OnField: mapAndFilterField(),
		OnValue: func(vc *VisitContext) error {
			if !vc.Leaf || vc.Value.Kind() == reflect.Invalid {
				return nil
			}
			return d.values.SetNestedPath(vc.MappedPath, dumpValue(vc))
		},
	})
	if err != nil {
		d.errors.Append(err)
	}
	return d
}

// Flat merges nested values, keys are concatenated with a dot.
func (d *Dumper) Flat() *Dumper {
	flat := orderedmap.New()
	d.values.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		if _, ok := value.(dumpedValue); ok {
			flat.Set(path.String(), value)
		}
	})
	flat.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	d.values = flat
	return d
}

// As formats the dumped values into the desired format.
func (d *Dumper) As(format string) ([]byte, error) {
	switch strings.ToLower(format) {
	case "json":
		return d.AsJSON(true)
	case "yml", "yaml":
		return d.AsYAML()
	default:
		return nil, errors.Errorf(`unexpected dump format "%s": expected "json", "yml" or "yaml"`, format)
	}
}

// AsJSON formats the dumped values into the JSON format.
func (d *Dumper) AsJSON(pretty bool) ([]byte, error) {
	if err := d.errors.ErrorOrNil(); err != nil {
		return nil, err
	}
	return json.Encode(d.values, pretty)
}

// AsYAML formats the dumped values into the YAML format.
func (d *Dumper) AsYAML() ([]byte, error) {
	if err := d.errors.ErrorOrNil(); err != nil {
		return nil, err
	}

	// Map each value to yaml.Node with optional HeadComment
	yamlMap := orderedmap.New()
	d.values.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		if v, ok := value.(dumpedValue); ok {
			node := &yaml.Node{}
			if err := node.Encode(v.Value); err == nil {
				node.HeadComment = v.HeadComment
				if err := yamlMap.SetNestedPath(path, node); err != nil {
					d.errors.Append(err)
				}
			} else {
				d.errors.Append(err)
			}
		}
	})
	if err := d.errors.ErrorOrNil(); err != nil {
		return nil, err
	}

	return yaml.Marshal(yamlMap)
}

func dumpValue(vc *VisitContext) (value dumpedValue) {
	switch {
	case vc.Value.Kind() == reflect.Pointer && vc.Value.IsNil():
		// Nil pointer -> null
		return dumpedValue{Value: nil, HeadComment: vc.Usage}
	case vc.PrimitiveValue.Kind() == reflect.Slice && vc.PrimitiveValue.IsNil():
		// Empty slice -> [] (not null)
		emptySlice := reflect.MakeSlice(vc.PrimitiveValue.Type(), 0, 0).Interface()
		return dumpedValue{Value: emptySlice, HeadComment: vc.Usage}
	case vc.Sensitive:
		// Mask sensitive field
		return dumpedValue{Value: sensitiveMask, HeadComment: vc.Usage}
	default:
		comment := vc.Usage
		if vc.Validate != "" {
			if comment != "" {
				comment = strings.TrimRight(comment, " .") + ". "
			}
			comment += "Validation rules: " + vc.Validate
		}
		return dumpedValue{Value: vc.PrimitiveValue.Interface(), HeadComment: comment}
	}
}
