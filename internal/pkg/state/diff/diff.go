// Package diff compares an A and B model.Objects collections, see Diff function.
package diff

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

type Differ struct {
	naming *naming.Registry
}

func Diff(A, B model.Objects, naming *naming.Registry) (*Result, error) {
	d := Differ{naming: naming}
	return d.Diff(A, B)
}

func NewDiffer(naming *naming.Registry) *Differ {
	return &Differ{naming: naming}
}

// Diff compares A and B model.Objects collections.
// Result are sorted according to the A collection, see model.Objects.Less function.
func (d *Differ) Diff(A, B model.Objects) (*Result, error) {
	out := &Result{A: A, B: B, Equal: true, Results: []*ResultObject{}, Errors: errors.NewMultiError(), naming: d.naming}

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
			Object{Key: key, Object: aObjects.GetOrNil(key), All: A},
			Object{Key: key, Object: bObjects.GetOrNil(key), All: B},
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
		return A.Less(out.Results[i].Key, out.Results[j].Key)
	})

	return out, out.Errors.ErrorOrNil()
}

func (d *Differ) diffObject(key model.Key, a, b Object) (*ResultObject, error) {
	result := &ResultObject{Key: key, A: a, B: b}

	// Are both, Remote and Local state defined?
	if a.Object == nil && b.Object == nil {
		panic(fmt.Errorf("both A and B are nil"))
	}

	// Only in B
	if a.Object == nil {
		result.State = ResultOnlyInB
		return result, nil
	}

	// Only in A
	if b.Object == nil {
		result.State = ResultOnlyInA
		return result, nil
	}

	// Get core type
	_, aType := coreType(reflect.ValueOf(a.Object))
	_, bType := coreType(reflect.ValueOf(b.Object))

	// A and B types must have same type
	if aType.String() != bType.String() {
		panic(fmt.Errorf("diff values A(%s) and B(%s) must have same data type", aType, bType))
	}

	// Diff
	reporter := newReporter(a, b, d.naming)
	cmp.Diff(a.Object, b.Object, reporter.Options())

	// Set results
	result.Values = reporter.Results()
	if len(result.Values) != 0 {
		result.State = ResultNotEqual
	}

	return result, nil
}
