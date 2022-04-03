package diff

import (
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
	naming            *naming.Registry
}

// ResultObject of diff of A and B model.Object.
type ResultObject struct {
	Key    model.Key
	A      Object
	B      Object
	State  ResultState
	Values []*ResultValue
}

type ResultValue struct {
	A      reflect.Value
	B      reflect.Value
	State  ResultState
	Path   Path
	FsPath *model.AbsPath // filled in if the record is related to a file in the filesystem
}
