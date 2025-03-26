package input

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	KindInput         = Kind("input")
	KindHidden        = Kind("hidden")
	KindTextarea      = Kind("textarea")
	KindConfirm       = Kind("confirm")
	KindSelect        = Kind("select")
	KindMultiSelect   = Kind("multiselect")
	KindOAuth         = Kind("oauth")
	KindOAuthAccounts = Kind("oauthAccounts")
)

// Kind represents how Input is displayed to the user.
// For example, TypeString can be displayed as text input or select box.
type Kind string

type Kinds []Kind

func allKinds() Kinds {
	return Kinds{KindInput, KindHidden, KindTextarea, KindConfirm, KindSelect, KindMultiSelect, KindOAuth, KindOAuthAccounts}
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

// ValidateType validates that type is valid for kind.
func (k Kind) ValidateType(t Type) error {
	switch k {
	case KindInput:
		if t != TypeString && t != TypeInt && t != TypeDouble {
			return errors.Errorf("should be one of [string, int, double] for kind=%s, found %s", k, t)
		}
	case KindHidden:
		if t != TypeString {
			return errors.Errorf("should be string for kind=%s, found %s", k, t)
		}
	case KindTextarea:
		if t != TypeString {
			return errors.Errorf("should be string for kind=%s, found %s", k, t)
		}
	case KindConfirm:
		if t != TypeBool {
			return errors.Errorf("should be bool for kind=%s, found %s", k, t)
		}
	case KindSelect:
		if t != TypeString {
			return errors.Errorf("should be string for kind=%s, found %s", k, t)
		}
	case KindMultiSelect:
		if t != TypeStringArray {
			return errors.Errorf("should be string[] for kind=%s, found %s", k, t)
		}
	case KindOAuth:
		if t != TypeObject {
			return errors.Errorf("should be object for kind=%s, found %s", k, t)
		}
	case KindOAuthAccounts:
		if t != TypeObject {
			return errors.Errorf("should be object for kind=%s, found %s", k, t)
		}
	default:
		panic(errors.Errorf(`unexpected kind "%s"`, t))
	}

	return nil
}
