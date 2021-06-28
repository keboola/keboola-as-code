package validator

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"keboola-as-code/src/utils"
	"reflect"
	"regexp"
	"strings"
)

func Validate(value interface{}) error {
	// Setup
	validate := validator.New()
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
		return processValidateError(err.(validator.ValidationErrors))
	}
	return nil
}

func processValidateError(err validator.ValidationErrors) error {
	result := &utils.Error{}
	for _, e := range err {
		// Remove struct name, first part
		namespace := regexp.MustCompile(`^([^.]+\.)?(.*)$`).ReplaceAllString(e.Namespace(), `$2`)
		// Hide nested fields
		namespace = strings.ReplaceAll(namespace, `__nested__.`, ``)
		result.Add(fmt.Errorf(
			"key=\"%s\", value=\"%v\", failed \"%s\" validation",
			namespace,
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
