// Package validator is a wrapper around go-playground/validator library.
// A custom validation rules (tags) can be specified.
//
// Note:
// Setting up a translator in the go-validator library is a bit tricky.
// This package has it solved. See library examples if needed:
// https://github.com/go-playground/validator/blob/master/_examples/translations
package validator

import (
	"context"
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

const (
	DisableRequiredInProjectKey = contextKey(`disable_required_in_project`) // disables "required_in_project" validation by Context
	anonymousField              = "__anonymous__"
)

// Rule is custom validation rule/tag.
type Rule struct {
	Tag          string
	Func         validator.Func
	FuncCtx      validator.FuncCtx
	ErrorMessage string
}

type contextKey string

// Validate nested struct fields or slice items.
func Validate(ctx context.Context, value interface{}, rules ...Rule) error {
	return ValidateCtx(ctx, value, "dive", "", rules...)
}

// ValidateCtx validates any value.
func ValidateCtx(ctx context.Context, value interface{}, tag string, namespace string, rules ...Rule) error {
	v := newValidator()
	v.registerRule(rules...)

	// nolint: errorlint // library always returns validator.ValidationErrors
	if err := v.validator.VarCtx(ctx, value, tag); err != nil {
		return v.formatError(err.(validator.ValidationErrors), namespace, reflect.ValueOf(value))
	}
	return nil
}

type wrapper struct {
	validator  *validator.Validate
	translator ut.Translator
}

func newValidator() *wrapper {
	// Create validator and translator
	v := &wrapper{
		validator:  validator.New(),
		translator: ut.New(en.New()).GetFallback(),
	}

	v.registerCustomRules()

	// Register error messages
	v.registerDefaultErrorMessages()
	v.registerErrorMessage("required_if", "{0} is a required field")

	// Modify fields names in error "namespace".
	v.validator.RegisterTagNameFunc(func(fld reflect.StructField) string {
		// Use "anonymousField" name for anonymous fields, so they can be removed from the error namespace.
		// See "formatError" method.
		if fld.Anonymous {
			return anonymousField
		}

		// Prefer JSON field name in error messages
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" {
			return fld.Name
		}
		return name
	})

	return v
}

// registerRule by tag and function.
func (v *wrapper) registerRule(rules ...Rule) {
	for _, rule := range rules {
		// Register validation function
		switch {
		case rule.FuncCtx != nil:
			if err := v.validator.RegisterValidationCtx(rule.Tag, rule.FuncCtx); err != nil {
				panic(err)
			}
		case rule.Func != nil:
			if err := v.validator.RegisterValidation(rule.Tag, rule.Func); err != nil {
				panic(err)
			}
		default:
			panic(fmt.Errorf(`please specify validator.Rulw.FuncCtx or Func`))
		}

		// Register error message
		v.registerErrorMessage(rule.Tag, rule.ErrorMessage)
	}
}

func (v *wrapper) registerCustomRules() {
	v.registerRule(
		// Register default validation for "required_in_project"
		// Some values are required in the project scope, but ignored in the template scope.
		// We validate them by default.
		Rule{
			Tag: "required_in_project",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				if v, _ := ctx.Value(DisableRequiredInProjectKey).(bool); v {
					// Template mode, value is valid.
					return true
				}
				// Project mode, value must be set.
				return !fl.Field().IsZero()
			},
			ErrorMessage: "{0} is a required field",
		},
		// Alphanumeric string with allowed dash character.
		Rule{
			Tag: "alphanumdash",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				return regexpcache.MustCompile(`[a-zA-Z0-9\-]+$`).MatchString(fl.Field().String())
			},
			ErrorMessage: "{0} can only contain alphanumeric characters and dash",
		},
	)
}

// registerErrorMessage for a tag.
func (v *wrapper) registerErrorMessage(tag, message string) {
	if tag == "" {
		panic(fmt.Errorf(`tag cannot be empty`))
	}
	if message == "" {
		panic(fmt.Errorf(`message cannot be empty`))
	}

	registerFn := func(ut ut.Translator) error {
		return ut.Add(tag, message, true)
	}
	translationFn := func(ut ut.Translator, fe validator.FieldError) string {
		t, err := ut.T(fe.Tag(), fe.Field())
		if err != nil {
			panic(err)
		}
		return t
	}
	if err := v.validator.RegisterTranslation(tag, v.translator, registerFn, translationFn); err != nil {
		panic(err)
	}
}

func (v *wrapper) registerDefaultErrorMessages() {
	if err := enTranslation.RegisterDefaultTranslations(v.validator, v.translator); err != nil {
		panic(err)
	}
}

// formatError creates human-readable error message.
func (v *wrapper) formatError(err validator.ValidationErrors, namespace string, value reflect.Value) error {
	result := utils.NewMultiError()
	for _, e := range err {
		// Translate error
		errString := strings.TrimSpace(e.Translate(v.translator))
		if strings.HasPrefix(errString, "Key: ") {
			// Use generic message if the error has not been translated
			errString = fmt.Sprintf("%s is invalid", e.Field())
		}

		// Prefix error with namespace
		result.Append(fmt.Errorf("%s", prefixErrorWithNamespace(e, errString, namespace, value)))
	}

	return result.ErrorOrNil()
}

// processNamespace removes struct name (first part), field name (last part) and anonymous fields.
func prefixErrorWithNamespace(e validator.FieldError, errMsg string, customNamespace string, value reflect.Value) string {
	// Remove anonymous fields
	errNamespace := strings.ReplaceAll(e.Namespace(), anonymousField+".", "")

	// Split error namespace
	var namespaceParts []string
	if errNamespace != "" {
		namespaceParts = strings.Split(errNamespace, ".")
	}

	// Is field present at the beginning of the error message?
	errMsgFirstWord := strings.SplitN(errMsg, " ", 2)[0]
	errMsgContainsField := len(namespaceParts) > 0 && namespaceParts[len(namespaceParts)-1] == errMsgFirstWord

	// Remove first part, if it is a struct name
	valueType := value.Type()
	for valueType.Kind() == reflect.Ptr {
		// Type to which the pointer refers
		valueType = valueType.Elem()
	}
	if valueType.Kind() == reflect.Struct && namespaceParts[0] == valueType.Name() {
		namespaceParts = namespaceParts[1:]
	}

	// Remove field name from namespace, if it is present in the error message
	if errMsgContainsField {
		namespaceParts = namespaceParts[:len(namespaceParts)-1]
	}

	// Prepend custom namespace
	if customNamespace != "" {
		namespaceParts = append(strings.Split(customNamespace, "."), namespaceParts...)
	}

	// Prefix error message with namespace
	namespace := strings.Join(namespaceParts, ".")
	switch {
	case namespace == "":
		return errMsg
	case errMsgContainsField:
		return namespace + "." + errMsg
	default:
		return namespace + " " + errMsg
	}
}
