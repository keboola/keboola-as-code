// Package example provides improved examples generator for Goa framework.
//
// Improvements:
//   - The order of the fields is preserved.
//   - Examples from low-level custom types are correctly used in composited types.
//   - Examples for ArrayOf definition contain one example of the element type.
package example

import (
	"encoding/json"
	"reflect"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"goa.design/goa/v3/codegen"
	"goa.design/goa/v3/eval"
	"goa.design/goa/v3/expr"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	metaVisitedAttr      = "ext:example:visited"
	metaVisitedAttrValue = "true"
)

//nolint:gochecknoinits
func init() {
	codegen.RegisterPluginFirst("001-example-object", "gen", prepare, generate)
}

func prepare(_ string, roots []eval.Root) error {
	return visitRoots(roots)
}

func generate(_ string, _ []eval.Root, files []*codegen.File) ([]*codegen.File, error) {
	return files, nil
}

func visitRoots(roots []eval.Root) error {
	for _, root := range roots {
		if rootExpr, ok := root.(*expr.RootExpr); ok {
			if err := visitRoot(rootExpr); err != nil {
				return err
			}
		}
	}
	return nil
}

func visitRoot(root *expr.RootExpr) error {
	for _, item := range allAttributes(root) {
		if item == nil {
			continue
		}

		err := codegen.Walk(item, func(attr *expr.AttributeExpr) error {
			// Visit each attribute only once
			if v, _ := attr.Meta.Last(metaVisitedAttr); v == metaVisitedAttrValue {
				return nil
			}
			attr.AddMeta(metaVisitedAttr, metaVisitedAttrValue)

			// Process attribute examples
			examples := attr.ExtractUserExamples()
			if len(examples) == 0 {
				// Generate example
				if v, err := exampleFor(attr, root.API.ExampleGenerator); err == nil {
					attr.UserExamples = append(attr.UserExamples, &expr.ExampleExpr{Summary: "default", Value: v})
				} else {
					return err
				}
				return nil
			}

			// Normalize already defined examples
			for _, example := range examples { // example is a pointer
				if v, err := normalizeExample(example.Value); err == nil {
					example.Value = v
				} else {
					return err
				}
			}
			attr.UserExamples = examples

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func allAttributes(root *expr.RootExpr) (attrs []*expr.AttributeExpr) {
	root.WalkSets(func(set eval.ExpressionSet) {
		for _, item := range set {
			switch v := item.(type) {
			case expr.CompositeExpr:
				attrs = append(attrs, v.Attribute())
			case *expr.AttributeExpr:
				attrs = append(attrs, v)
			case *expr.MethodExpr:
				attrs = append(attrs, v.Payload)
				attrs = append(attrs, v.StreamingPayload)
				attrs = append(attrs, v.Result)
			case *expr.HTTPEndpointExpr:
				attrs = append(attrs, v.Body)
				attrs = append(attrs, v.StreamingBody)
				for _, res := range v.Responses {
					attrs = append(attrs, res.Body)
				}
			default:
				continue
			}
		}
	})
	return attrs
}

func exampleFor(attr *expr.AttributeExpr, g *expr.ExampleGenerator) (any, error) {
	if values := attr.ExtractUserExamples(); len(values) > 0 {
		return normalizeExample(values[len(values)-1].Value)
	}

	switch value := attr.Type.(type) {
	case *expr.UserTypeExpr:
		// Step down
		return exampleFor(value.AttributeExpr, g)
	case *expr.Object:
		// Generate example for each field
		out := orderedmap.New()
		for _, f := range *value {
			if example, err := exampleFor(f.Attribute, g); err == nil {
				out.Set(f.Name, example)
			} else {
				return nil, err
			}
		}
		return out, nil
	case *expr.Array:
		var out []any
		userExamples := value.ElemType.ExtractUserExamples()
		if len(userExamples) > 0 {
			// Use UserExamples of the element type
			for _, item := range userExamples {
				if example, err := normalizeExample(item.Value); err == nil {
					out = append(out, example)
				} else {
					return nil, err
				}
			}
			return out, nil
		}

		// Generate array with one example
		if example, err := exampleFor(value.ElemType, g); err == nil {
			out = append(out, example)
		} else {
			return nil, err
		}

		return out, nil
	}

	return attr.Example(g), nil
}

// normalizeExample converts complex structures to map using JSON serialization.
func normalizeExample(in any) (any, error) {
	if _, ok := in.(*orderedmap.OrderedMap); ok {
		return in, nil
	}

	// Must implements JSON marshaling
	if _, ok := in.(json.Marshaler); !ok {
		return in, nil
	}

	// Dereference pointer, if any
	t := reflect.ValueOf(in).Type()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	// Value must be struct or array
	if t.Kind() != reflect.Struct && t.Kind() != reflect.Slice {
		return in, nil
	}

	// Convert the input using JSON
	bytes, err := json.Marshal(in)
	if err != nil {
		return nil, errors.Errorf(`cannot marshal example struct "%T" to JSON: %w`, in, err)
	}
	switch t.Kind() {
	case reflect.Struct:
		out := orderedmap.New()
		if err := json.Unmarshal(bytes, out); err != nil {
			return nil, errors.Errorf(`cannot unmarshal example struct "%T" from JSON: %w`, in, err)
		}
		return out, nil
	case reflect.Slice:
		var out []*orderedmap.OrderedMap
		if err := json.Unmarshal(bytes, &out); err != nil {
			return nil, errors.Errorf(`cannot unmarshal example struct "%T" from JSON: %w`, in, err)
		}
		// Convert []*orderedmap.OrderedMap to []any
		var items []any
		for _, item := range out {
			items = append(items, item)
		}
		return items, nil
	default:
		panic(errors.New("unexpected type"))
	}
}
