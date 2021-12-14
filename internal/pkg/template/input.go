package template

import (
	"reflect"

	goValidator "github.com/go-playground/validator/v10"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Inputs []*Input

type Input struct {
	Id          string                 `json:"id" validate:"required,template-input-id"`
	Name        string                 `json:"name" validate:"required"`
	Description string                 `json:"description" validate:"required"`
	Type        string                 `json:"type" validate:"required"`
	Default     interface{}            `json:"default,omitempty" validate:"omitempty,template-input-default"`
	Kind        string                 `json:"kind" validate:"required,oneof=input password textarea confirm select multiselect"`
	Options     []Option               `json:"options,omitempty" validate:"required_if=Type select Type multiselect,template-input-options,dive,template-input-option"`
	Rules       *orderedmap.OrderedMap `json:"rules,omitempty"`
	If          string                 `json:"if,omitempty"`
}

type Option interface{}

func (i Inputs) Validate() error {
	validations := []validator.Validation{
		{
			Tag:  "template-input-id",
			Func: validateId,
		},
		{
			Tag:  "template-input-default",
			Func: validateDefault,
		},
		{
			Tag:  "template-input-options",
			Func: validateOptions,
		},
		{
			Tag:  "template-input-option",
			Func: validateOption,
		},
	}
	return validator.Validate(i, validations...)
}

func validateId(fl goValidator.FieldLevel) bool {
	return regexpcache.MustCompile(`^[a-zA-Z\.\_]+$`).MatchString(fl.Field().String())
}

// Default value must be of the same type as the Type.
func validateDefault(fl goValidator.FieldLevel) bool {
	if fl.Field().Kind() == reflect.Ptr && fl.Field().IsNil() {
		return true
	}
	if fl.Field().IsZero() {
		return true
	}
	return fl.Field().Kind().String() == fl.Parent().FieldByName("Type").String()
}

// Options must be filled only for select or multiselect Kind.
func validateOptions(fl goValidator.FieldLevel) bool {
	if fl.Parent().FieldByName("Kind").String() == "select" || fl.Parent().FieldByName("Kind").String() == "multiselect" {
		return fl.Field().Len() > 0
	}
	return fl.Field().Len() == 0
}

// Each Option must be of the type defined in Type.
func validateOption(fl goValidator.FieldLevel) bool {
	return fl.Parent().FieldByName("Type").String() == fl.Field().Kind().String()
}
