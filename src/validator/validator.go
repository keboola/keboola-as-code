package validator

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"keboola-as-code/src/utils"
	"reflect"
	"strings"
)

func Validate(value interface{}) error {
	// Setup
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		// Use JSON field name in error messages
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return fld.Name
		}
		return name
	})

	// Do
	if err := validate.Struct(value); err != nil {
		return processValidateError(err.(validator.ValidationErrors))
	}
	return nil
}

func processValidateError(err validator.ValidationErrors) error {
	result := &utils.Error{}
	for _, e := range err {
		path := strings.TrimPrefix(e.Namespace(), "Manifest.")
		result.Add(fmt.Errorf(
			"key=\"%s\", value=\"%v\", failed \"%s\" validation",
			path,
			e.Value(),
			e.ActualTag(),
		))
	}

	// Convert msg to error
	if result.Len() > 0 {
		return result
	}
	return nil

}
