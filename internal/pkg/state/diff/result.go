package diff

import (
	"fmt"
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
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
	Object *model.ObjectLeaf
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
	naming            *naming.Registry
}

// ResultObject of diff of A and B model.Object.
type ResultObject struct {
	model.Key
	A      Object
	B      Object
	State  ResultState
	Values []*ResultValue
}

type ResultValue struct {
	A     reflect.Value
	B     reflect.Value
	State ResultState
	path  []string
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
