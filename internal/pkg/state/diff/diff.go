// Package diff compares an A and B model.Objects collections, see Diff function.
package diff

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	objectsSort "github.com/keboola/keboola-as-code/internal/pkg/state/sort"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type Differ interface {
	Diff(A, B model.Objects) (*Result, error)
}

type differ struct {
	sorter model.ObjectsSorter
}

type Option func(cfg *differ)

func WithSorter(v model.ObjectsSorter) Option {
	return func(d *differ) {
		d.sorter = v
	}
}

// Diff compares A and B model.Objects collections.
// Result is sorted according to the A collection, see model.Objects.Less function.
func Diff(A, B model.Objects, opts ...Option) (*Result, error) {
	return NewDiffer(opts...).Diff(A, B)
}

func NewDiffer(opts ...Option) Differ {
	d := &differ{}

	// Apply options
	for _, o := range opts {
		o(d)
	}

	// Create default sorter if needed
	if d.sorter == nil {
		d.sorter = objectsSort.NewIdSorter()
	}
	return d
}

// Diff compares A and B model.Objects collections.
// Result is sorted according to the A collection, see model.Objects.Less function.
func (d *differ) Diff(A, B model.Objects) (*Result, error) {
	out := &Result{A: A, B: B, Equal: true, Results: []*ResultObject{}, Errors: errors.NewMultiError()}

	// Find all keys present in A, B or both.
	keys := make(map[model.Key]bool)
	aObjects, bObjects := A.AllAsTree(), B.AllAsTree()
	for _, objects := range []model.ObjectsTree{aObjects, bObjects} {
		for _, object := range objects.Root() {
			if key := object.Key(); !keys[key] {
				keys[key] = true
			}
		}
	}

	// Diff each object
	for key := range keys {
		result, err := d.diffObject(
			key,
			Object{node: aObjects.GetOrNil(key), All: A},
			Object{node: bObjects.GetOrNil(key), All: B},
		)

		// Handle error
		if err != nil {
			out.Errors.Append(err)
			continue
		}

		// Update global state
		switch result.State {
		case ResultNotEqual:
			out.Equal = false
			out.HasNotEqualResult = true
		case ResultOnlyInA:
			out.Equal = false
			out.HasOnlyInAResult = true
		case ResultOnlyInB:
			out.Equal = false
			out.HasOnlyInBResult = true
		}

		out.Results = append(out.Results, result)
	}

	// Sort results according to the A collection
	sort.SliceStable(out.Results, func(i, j int) bool {
		return d.sorter.Less(out.Results[i].Key, out.Results[j].Key)
	})

	return out, out.Errors.ErrorOrNil()
}

func (d *differ) diffObject(key model.Key, a, b Object) (*ResultObject, error) {
	result := &ResultObject{Key: key, A: a, B: b}

	// Are both, Remote and Local state defined?
	if a.IsNil() && b.IsNil() {
		panic(fmt.Errorf("both A and B are nil"))
	}

	// Only in B
	if a.IsNil() {
		result.State = ResultOnlyInB
		return result, nil
	}

	// Only in A
	if b.IsNil() {
		result.State = ResultOnlyInA
		return result, nil
	}

	// Get core type
	_, aType := coreType(reflect.ValueOf(a.ObjectNode()))
	_, bType := coreType(reflect.ValueOf(b.ObjectNode()))

	// A and B types must have same type
	if aType.String() != bType.String() {
		panic(fmt.Errorf("diff values A(%s) and B(%s) must have same data type", aType, bType))
	}

	// Diff
	reporter := newReporter(a, b)
	cmp.Diff(a.ObjectNode(), b.ObjectNode(), options(reporter))

	// Set results
	result.Differences = reporter.Differences()
	if len(result.Differences) == 0 {
		result.State = ResultEqual
	} else {
		result.State = ResultNotEqual
	}

	return result, nil
}

// options to modify diff process.
func options(r *Reporter) cmp.Options {
	return cmp.Options{
		cmp.Reporter(r),
		// Diff only struct fields with diff:"true" tag
		onlyMarkedWithDiffTag(),
		// Compare ordered map as native map (keys order doesn't matter)
		orderedMapToMapTransformer(),
		// Convert []Object -> Object, if parent Object can have only one child Object of a Kind.
		// Convert []Object -> map[Key]Object, so objects with the same key are compared with each other regardless of the order in the slice.
		objectsSliceTransformer(),
	}
}

// onlyMarkedWithDiffTag ignores struct field without diff:"true" tag
func onlyMarkedWithDiffTag() cmp.Option {
	return cmp.FilterPath(
		func(path cmp.Path) bool {
			previousIndex := len(path) - 2
			if previousIndex > 0 {
				if v, ok := path.Last().(cmp.StructField); ok {
					parentType := path.Index(len(path) - 2).Type()
					currentField, _ := parentType.FieldByName(v.Name())
					return currentField.Tag.Get("diff") != "true"
				}
			}
			// Allow
			return false
		},
		cmp.Ignore(),
	)
}

// orderedMapToMapTransformer converts "orderedmap" type to native map, so keys order doesn't matter.
func orderedMapToMapTransformer() cmp.Option {
	return cmp.Transformer("orderedMap", func(m *orderedmap.OrderedMap) map[string]interface{} {
		return m.ToMap()
	})
}

// objectsSliceTransformer has 2 functions
// 1: Converts []Object -> Object, if parent Object can have only one child Object of a Kind.
// 2: Converts []Object -> map[Key]Object, so objects with the same key are compared with each other regardless of the order in the slice.
func objectsSliceTransformer() cmp.Option {
	return cmp.Transformer("objectsSliceToMap", func(children []*model.ObjectNode) interface{} {
		// Convert []Object -> Object, if parent Object can have only one child Object of a Kind.
		if len(children) == 1 && !children[0].Kind().ToMany {
			return children[0]
		}

		// Convert []Object -> Object, if parent Object can have only one child Object of a Kind.
		out := make(map[model.Key]*model.ObjectNode)
		for _, o := range children {
			out[o.Key()] = o
		}
		return out
	})
}
