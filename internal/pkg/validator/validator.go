package validator

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTranslation "github.com/go-playground/validator/v10/translations/en"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Validation struct {
	Tag  string
	Func validator.Func
}

func newValidator(rules ...Validation) (*validator.Validate, ut.Translator) {
	validate := validator.New()
	enLocale := en.New()
	universalTranslator := ut.New(enLocale, enLocale)
	enTranslator, found := universalTranslator.GetTranslator("en")
	if !found {
		panic(fmt.Errorf("en translator was not found"))
	}
	err := enTranslation.RegisterDefaultTranslations(validate, enTranslator)
	if err != nil {
		panic(fmt.Errorf("translator was not registered: %w", err))
	}

	for _, rule := range rules {
		err := validate.RegisterValidation(rule.Tag, rule.Func)
		if err != nil {
			panic(err)
		}
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
	return validate, enTranslator
}

func Validate(value interface{}, rules ...Validation) error {
	return ValidateCtx(value, context.Background(), "dive", "", rules...)
}

func ValidateCtx(value interface{}, ctx context.Context, tag string, fieldName string, rules ...Validation) error {
	validate, enTranslator := newValidator(rules...)

	if err := validate.VarCtx(ctx, value, tag); err != nil {
		var validationErrs validator.ValidationErrors
		switch {
		case errors.As(err, &validationErrs):
			return processValidateError(validationErrs, enTranslator, fieldName)
		default:
			panic(err)
		}
	}

	return nil
}

// Remove struct name (first part), field name (last part) and __nested__ parts.
func processNamespace(namespace string) string {
	namespace = strings.ReplaceAll(namespace, `__nested__.`, ``)
	parts := strings.Split(namespace, ".")
	if len(parts) <= 2 {
		return ""
	}
	return strings.Join(parts[1:len(parts)-1], ".")
}

func processValidateError(err validator.ValidationErrors, translator ut.Translator, fieldName string) error {
	result := utils.NewMultiError()
	for _, e := range err {
		errorFieldName := fieldName
		// Prefix error message by field namespace
		if namespace := processNamespace(e.Namespace()); namespace != "" {
			errorFieldName = fmt.Sprintf("%s.", namespace)
		}
		result.Append(fmt.Errorf("%s%s", errorFieldName, e.Translate(translator)))
	}

	return result.ErrorOrNil()
}
