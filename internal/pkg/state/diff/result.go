package diff

import (
	"reflect"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// All possible diff results.
const (
	ResultNotSet ResultState = iota
	ResultNotEqual
	ResultEqual
	ResultOnlyInA
	ResultOnlyInB
)

type node = model.ObjectNode

// Object from A or B model.Objects collection, contains reference to all objects.
// Node field can be nil, if Object is not present in the collection.
type Object struct {
	*node
	All model.Objects
}

type ResultState int

// Result of diff of A and B model.Objects collections.
type Result struct {
	A                 model.Objects      // all A objects
	B                 model.Objects      // all B objects
	Results           []*ResultObject    // diff results
	Errors            *errors.MultiError // diff errors
	Equal             bool               // all objects are equal
	HasNotEqualResult bool               // there is at least one ResultNotEqual result
	HasOnlyInAResult  bool               // there is at least one ResultOnlyInA
	HasOnlyInBResult  bool               // there is at least one ResultOnlyInB
}

// ResultObject of diff of A and B model.Object.
type ResultObject struct {
	Key         model.Key
	A           Object
	B           Object
	State       ResultState
	Differences ResultItems
}

type ResultItems []*ResultItem

type ResultItem struct {
	A     ResultValue
	B     ResultValue
	State ResultState
	Path  Path
}

type ResultValue struct {
	Original    reflect.Value // value before applying all cmp.Transform
	Transformed reflect.Value // value after applying all cmp.Transform
}

type ValuesPair struct {
	A interface{} // value from the A collection
	B interface{} // value from the B collection
}

func NewResultValue(v reflect.Value) ResultValue {
	// At the beginning, Original and Transformed are the same.
	// Transformed is later changed using step.AddTransform method.
	return ResultValue{
		Original:    v,
		Transformed: v,
	}
}

func (v *Result) Get(key model.Key) (*ResultObject, bool) {
	for _, object := range v.Results {
		if object.Key == key {
			return object, true
		}
	}
	return nil, false
}

func (v *ResultObject) AOrBObject() model.Object {
	if object := v.A.Object(); object != nil {
		return object
	}
	return v.B.Object()
}

func (v ResultItems) IsEmpty() bool {
	return len(v) == 0
}

// String returns all paths separated by comma.
// For example: "name, configuration.key1, configuration.key2".
func (v ResultItems) String() string {
	var paths []string
	for _, item := range v {
		paths = append(paths, item.Path.String())
	}
	sort.Strings(paths)
	return strings.Join(paths, ", ")
}

// ShortString returns first part of all paths separated by comma.
// For example: "name, configuration".
func (v ResultItems) ShortString() string {
	uniquePaths := make(map[string]bool)
	for _, item := range v {
		uniquePaths[item.Path.First().String()] = true
	}

	var paths []string
	for path := range uniquePaths {
		paths = append(paths, path)
	}

	sort.Strings(paths)
	return strings.Join(paths, ", ")
}

func (v ResultValue) IsValid() bool {
	return v.Original.IsValid()
}

func (v *Object) Object() model.Object {
	if v.node == nil {
		return nil
	}
	return v.node.Object
}

func (v *Object) Children() model.ObjectChildren {
	if v.node == nil {
		return nil
	}
	return v.node.Children
}

func (v *Object) ObjectNode() *model.ObjectNode {
	return v.node
}

func (v *Object) IsNil() bool {
	return v.node == nil
}
