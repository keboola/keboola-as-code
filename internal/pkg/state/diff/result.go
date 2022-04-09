package diff

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// Possible diff results.
const (
	ResultNotSet ResultState = iota
	ResultNotEqual
	ResultEqual
	ResultOnlyInA
	ResultOnlyInB
)

type node = model.ObjectNode

// Object from A or B model.Objects collection, contains reference to all objects.
// Object field can be nil.
type Object struct {
	*node
	All model.Objects
}

type ResultState int

// Result of diff of A and B model.Objects collections.
type Result struct {
	A                 model.Objects
	B                 model.Objects
	Results           []*ResultObject
	Errors            *errors.MultiError
	Equal             bool
	HasNotEqualResult bool
	HasOnlyInAResult  bool
	HasOnlyInBResult  bool
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
	A     reflect.Value
	B     reflect.Value
	State ResultState
	Path  Path
}

func (v ResultState) Mark() string {
	switch v {
	case ResultNotEqual:
		return NotEqualMark
	case ResultEqual:
		return EqualMark
	case ResultOnlyInA:
		return OnlyInAMark
	case ResultOnlyInB:
		return OnlyInBMark
	default:
		panic(fmt.Errorf("unexpected value %#v", v))
	}
}

func (v *Result) Format(opts ...FormatOption) string {
	return format(v, opts...)
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
func (v ResultItems) String() string {
	var paths []string
	for _, item := range v {
		paths = append(paths, item.Path.String())
	}

	sort.Strings(paths)
	return strings.Join(paths, ", ")
}

// ShortString returns first part of all paths separated by comma.
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
