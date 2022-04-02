// Package diff compares an A and B model.Objects collections, see Diff function.
package diff

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Possible diff results.
const (
	ResultNotSet ResultState = iota
	ResultNotEqual
	ResultEqual
	ResultOnlyInA
	ResultOnlyInB
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

// Object from A or B model.Objects collection, contains reference to all objects.
// Object field can be nil.
type Object struct {
	Key    model.Key
	Object model.Object
	All    model.Objects
}

type ResultState int

// Result of diff of A and B model.Object.
type Result struct {
	model.Key
	A             Object
	B             Object
	State         ResultState
	ChangedFields model.ChangedFields
}

// Results of diff of A and B model.Objects collections.
type Results struct {
	A                 model.Objects
	B                 model.Objects
	Results           []*Result
	Errors            *errors.MultiError
	Equal             bool
	HasNotEqualResult bool
	HasOnlyInAResult  bool
	HasOnlyInBResult  bool
}

// Global cache of types.
type (
	typeName string
	typeMap  map[typeName][]*utils.StructField
)

var (
	typeLock  = &sync.Mutex{}
	typeCache = make(typeMap) // reflection cache
)

type differ struct {
	naming *naming.Registry
}

func Diff(A, B model.Objects, naming *naming.Registry) (*Results, error) {
	d := differ{naming: naming}
	return d.diff(A, B)
}

// Diff compares A and B model.Objects collections.
// Results are sorted according to the A collection, see model.Objects.Less function.
func (d *differ) diff(A, B model.Objects) (*Results, error) {
	out := &Results{A: A, B: B, Equal: true, Results: []*Result{}, Errors: errors.NewMultiError()}

	// Find all objects present in A, B or both.
	allMap := make(map[model.Key]bool)
	aMap := make(map[model.Key]model.ObjectWithChildren)
	bMap := make(map[model.Key]model.ObjectWithChildren)
	all := make([]model.Key, 0)
	for _, collection := range []model.Objects{A, B} {
		for _, object := range collection.AllGrouped() {
			if key := object.Key(); !allMap[key] {
				allMap[key] = true
				all = append(all, key)
			}

			if collection == A {
				aMap[object.Key()] = object
			}
			if collection == B {
				bMap[object.Key()] = object
			}
		}
	}

	// Diff each object
	for _, key := range all {
		result, err := d.diffObject(
			key,
			Object{Key: key, Object: aMap[key], All: A},
			Object{Key: key, Object: bMap[key], All: B},
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

func (d *differ) diffObject(key model.Key, a, b Object) (*Result, error) {
	result := &Result{Key: key, A: a, B: b}
	result.ChangedFields = model.NewChangedFields()

	// Are both, Remote and Local state defined?
	if a.Object == nil && b.Object == nil {
		panic(fmt.Errorf("both A and B are nil"))
	}

	// Not in remote state
	if a.Object == nil {
		result.State = ResultOnlyInB
		return result, nil
	}

	// Not in local state
	if b.Object == nil {
		result.State = ResultOnlyInA
		return result, nil
	}

	aValue, aType := coreType(reflect.ValueOf(a.Object))
	bValue, bType := coreType(reflect.ValueOf(b.Object))

	// A and B types must have same type
	if aType.String() != bType.String() {
		panic(fmt.Errorf("diff values A(%s) and B(%s) must have same data type", aType, bType))
	}

	// Get available fields for diff, defined in `diff:"true"` tag in struct
	diffFields := getDiffFields(aType)
	if len(diffFields) == 0 {
		return nil, fmt.Errorf(`no field with tag "diff:true" in struct "%s"`, aType.String())
	}

	// Diff
	for _, field := range diffFields {
		reporter := d.diffValues(
			a, b,
			aValue.FieldByName(field.StructField.Name).Interface(),
			bValue.FieldByName(field.StructField.Name).Interface(),
		)
		diffStr := reporter.String()
		if len(diffStr) > 0 {
			result.ChangedFields.
				Add(strhelper.FirstLower(field.JsonName())).
				SetDiff(diffStr).
				AddPath(reporter.Paths()...)
		}
	}

	if len(result.ChangedFields) > 0 {
		result.State = ResultNotEqual
	} else {
		result.State = ResultEqual
	}

	return result, nil
}

func (d *differ) diffValues(a, b Object, aValue, bValue interface{}) *Reporter {
	reporter := newReporter(a, b, d.naming)
	cmp.Diff(aValue, bValue, options(reporter))
	return reporter
}

func (v ResultState) Mark() string {
	switch v {
	case ResultNotSet:
		return "?"
	case ResultNotEqual:
		return NotEqualMark
	case ResultEqual:
		return EqualMark
	case ResultOnlyInA:
		return OnlyInAMark
	case ResultOnlyInB:
		return OnlyInBMark
	default:
		panic(fmt.Errorf("unexpected type %T", v))
	}
}

func (r *Result) String() string {
	var out strings.Builder
	for _, field := range r.ChangedFields.All() {
		out.WriteString(fmt.Sprintf("%s:\n", field.Name()))
		out.WriteString(field.Diff() + "\n")
	}
	return strings.TrimRight(out.String(), "\n")
}

func (r *Results) Format(naming *naming.Registry, details bool) []string {
	var out []string
	for _, result := range r.Results {
		if result.State != ResultEqual {
			// Get path by key
			path := result.Key.String()
			if pathAbs, found := naming.PathByKey(result.Key); found {
				path = pathAbs.String()
			}

			// Message
			msg := fmt.Sprintf("%s %s %s", result.State.Mark(), result.Kind().Abbr, path)
			if !details && !result.ChangedFields.IsEmpty() {
				msg += " | changed: " + result.ChangedFields.String()
			}
			out = append(out, msg)

			// Changed fields
			if details {
				for _, line := range strings.Split(result.String(), "\n") {
					out = append(out, fmt.Sprintf("  %s", line))
				}
			}
		}
	}
	return out
}

func getDiffFields(t reflect.Type) []*utils.StructField {
	typeLock.Lock()
	defer typeLock.Unlock()

	if v, ok := typeCache[typeName(t.Name())]; ok {
		return v
	} else {
		diffFields := utils.GetFieldsWithTag("diff:true", t)
		name := typeName(t.Name())
		typeCache[name] = diffFields
		return diffFields
	}
}

// options defines customization of the diff process.
func options(reporter *Reporter) cmp.Options {
	return cmp.Options{
		cmp.Reporter(reporter),
		// Compare Config/ConfigRow configuration content ("orderedmap" type) as map (keys order doesn't matter)
		cmp.Transformer("orderedmap", func(m *orderedmap.OrderedMap) map[string]interface{} {
			return m.ToMap()
		}),
		// Separately compares the relations for the manifest and API side
		cmpopts.AcyclicTransformer("relations", func(relations model.Relations) model.RelationsBySide {
			return relations.RelationsBySide()
		}),
		// Diff transformation blocks as string
		cmp.Transformer("block", func(x model.ObjectWithChildren) interface{} {
			return "ok!!!" + cast.ToString(time.Now().Nanosecond())
		}),
		//cmp.Transformer("code", func(code *model.Code) *string {
		//	return codeToString(code, reporter.naming)
		//}),
		// Diff orchestrator phases as string
		//cmp.Transformer("phase", func(phase *model.Phase) *string {
		//	return phaseToString(phase, reporter.naming)
		//}),
		//cmp.Transformer("task", func(task *model.Task) *string {
		//	return taskToString(task, reporter.naming)
		//}),
		//// Diff SharedCode row as string
		//cmp.Transformer("sharedCodeRow", func(code *model.SharedCodeRow) *string {
		//	return sharedCodeRowTostring(code, reporter.naming)
		//}),
	}
}
