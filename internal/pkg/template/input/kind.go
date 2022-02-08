package input

import (
	"fmt"
	"strings"
)

const (
	KindInput       = Kind("input")
	KindPassword    = Kind("password")
	KindTextarea    = Kind("textarea")
	KindConfirm     = Kind("confirm")
	KindSelect      = Kind("select")
	KindMultiSelect = Kind("multiselect")
)

// Kind represents how Input is displayed to the user.
// For example, TypeString can be displayed as text input or select box.
type Kind string

type Kinds []Kind

func allKinds() Kinds {
	return Kinds{KindInput, KindPassword, KindTextarea, KindConfirm, KindSelect, KindMultiSelect}
}

func (v Kinds) String() string {
	return strings.Join(v.Strings(), ", ")
}

func (v Kinds) Strings() []string {
	out := make([]string, len(v))
	for i, item := range v {
		out[i] = string(item)
	}
	return out
}

func (k Kind) IsValid() bool {
	for _, v := range allKinds() {
		if v == k {
			return true
		}
	}
	return false
}

// ValidateType validates that type is valid for kind.
func (k Kind) ValidateType(t Type) error {
	switch k {
	case KindInput:
		if t != TypeString && t != TypeInt && t != TypeDouble {
			return fmt.Errorf("should be one of [string, int, double] for kind=%s, found %s", k, t)
		}
	case KindPassword:
		if t != TypeString {
			return fmt.Errorf("should be string for kind=%s, found %s", k, t)
		}
	case KindTextarea:
		if t != TypeString {
			return fmt.Errorf("should be string for kind=%s, found %s", k, t)
		}
	case KindConfirm:
		if t != TypeBool {
			return fmt.Errorf("should be bool for kind=%s, found %s", k, t)
		}
	case KindSelect:
		if t != TypeString {
			return fmt.Errorf("should be string for kind=%s, found %s", k, t)
		}
	case KindMultiSelect:
		if t != TypeStringArray {
			return fmt.Errorf("should be string[] for kind=%s, found %s", k, t)
		}
	default:
		panic(fmt.Errorf(`unexpected kind "%s"`, t))
	}

	return nil
}
