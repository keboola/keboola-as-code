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
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Validation struct {
	Tag  string
	Func validator.Func
}

func Validate(value interface{}, rules ...Validation) error {
	return ValidateCtx(value, context.Background(), "dive", "", rules...)
}

func ValidateCtx(value interface{}, ctx context.Context, tag string, fieldName string, rules ...Validation) error {
	// Setup
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

	// Do

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

func processNamespace(namespace string) string {
	// Remove struct name (first part)
	result := regexpcache.MustCompile(`^([^.]+\.)?(.*)$`).ReplaceAllString(namespace, `$2`)

	// Hide nested fields
	result = strings.ReplaceAll(result, `__nested__.`, ``)

	// Field with one level only does not need namespace
	lastDotIndex := strings.LastIndex(result, ".")
	if lastDotIndex == -1 {
		return ""
	}

	// Remove field name (last part) from the namespace
	return result[:lastDotIndex]
}

func processValidateError(err validator.ValidationErrors, translator ut.Translator, fieldName string) error {
	result := utils.NewMultiError()
	for _, e := range err {
		if e.Namespace() != "" {
			processedNamespace := processNamespace(e.Namespace())
			if processedNamespace != "" {
				fieldName = fmt.Sprintf("%s.", processedNamespace)
			}
		}
		result.Append(fmt.Errorf("%s%s", fieldName, e.Translate(translator)))
	}

	return result.ErrorOrNil()
}
