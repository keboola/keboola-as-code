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

// Object from A or B model.Objects collection, contains reference to all objects.
// Object field can be nil.
type Object struct {
	Key    model.Key
	Object *model.ObjectNode
	All    model.Objects
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
	FsPath      *model.AbsPath
	Differences ResultValues
}

type ResultValues []*ResultValue

type ResultValue struct {
	A      reflect.Value
	B      reflect.Value
	State  ResultState
	Path   Path
	FsPath *model.AbsPath // filled in if the record is related to a file in the filesystem
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

func (v *Result) String(o FormatOptions) string {
	return format(v, o)
}

func (v ResultValues) IsEmpty() bool {
	return len(v) == 0
}

// String returns all paths separated by comma.
func (v ResultValues) String() string {
	var paths []string
	for _, item := range v {
		paths = append(paths, item.Path.String())
	}

	sort.Strings(paths)
	return strings.Join(paths, ", ")
}

// ShortString returns first part of all paths separated by comma.
func (v ResultValues) ShortString() string {
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
