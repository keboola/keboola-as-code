package validator

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func Validate(value interface{}) error {
	// Setup
	validate := validator.New()
	err := validate.RegisterValidation("template-input-id", validateTemplateInputId)
	if err != nil {
		panic(err)
	}
	err = validate.RegisterValidation("template-input-default", validateTemplateInputDefault)
	if err != nil {
		panic(err)
	}
	err = validate.RegisterValidation("template-input-options", validateTemplateInputOptions)
	if err != nil {
		panic(err)
	}
	err = validate.RegisterValidation("template-input-option", validateTemplateInputOption)
	if err != nil {
		panic(err)
	}
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		if fld.Anonymous {
			return "__nested__"
		}

		// Use JSON field name in error messages
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return fld.Name
		}
		return name
	})

	// Do
	if err := validate.Struct(value); err != nil {
		var validationErrs validator.ValidationErrors
		switch {
		case errors.As(err, &validationErrs):
			return processValidateError(validationErrs)
		default:
			panic(err)
		}
	}

	return nil
}

func processValidateError(err validator.ValidationErrors) error {
	result := utils.NewMultiError()
	for _, e := range err {
		// Remove struct name, first part
		namespace := regexpcache.MustCompile(`^([^.]+\.)?(.*)$`).ReplaceAllString(e.Namespace(), `$2`)
		// Hide nested fields
		namespace = strings.ReplaceAll(namespace, `__nested__.`, ``)
		result.Append(fmt.Errorf(
			"key=\"%s\", value=\"%v\", failed \"%s\" validation",
			namespace,
			e.Value(),
			e.ActualTag(),
		))
	}

	return result.ErrorOrNil()
}

func validateTemplateInputId(fl validator.FieldLevel) bool {
	return regexpcache.MustCompile(`^[a-zA-Z\.\_]+$`).MatchString(fl.Field().String())
}

// Default value must be of the same type as the Type.
func validateTemplateInputDefault(fl validator.FieldLevel) bool {
	if fl.Field().Kind() == reflect.Ptr && fl.Field().IsNil() {
		return true
	}
	if fl.Field().IsZero() {
		return true
	}
	return fl.Field().Kind().String() == fl.Parent().FieldByName("Type").String()
}

// Options must be filled only for select or multiselect Kind.
func validateTemplateInputOptions(fl validator.FieldLevel) bool {
	if fl.Parent().FieldByName("Kind").String() == "select" || fl.Parent().FieldByName("Kind").String() == "multiselect" {
		return fl.Field().Len() > 0
	}
	return fl.Field().Len() == 0
}

// Each Option must be of the type defined in Type.
func validateTemplateInputOption(fl validator.FieldLevel) bool {
	return fl.Parent().FieldByName("Type").String() == fl.Field().Kind().String()
}
