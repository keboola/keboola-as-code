package diff

import (
	"fmt"
	"strings"

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
	naming            *naming.Registry
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

func (r *Result) String() string {
	var out strings.Builder
	for _, field := range r.ChangedFields.All() {
		out.WriteString(fmt.Sprintf("%s:\n", field.Name()))
		out.WriteString(field.Diff() + "\n")
	}
	return strings.TrimRight(out.String(), "\n")
}

func (r *Results) Format(details bool) []string {
	var out []string
	for _, result := range r.Results {
		if result.State != ResultEqual {
			// Get path by key
			path := result.Key.String()
			if pathAbs, found := r.naming.PathByKey(result.Key); found {
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
