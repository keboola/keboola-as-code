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

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	DisableRequiredInProjectKey = contextKey(`disable_required_in_project`) // disables "required_in_project" validation by Context
	anonymousField              = "__anonymous__"
)

type Validator interface {
	RegisterRule(rules ...Rule)
	ValidateValue(value any, tag string) error
	Validate(ctx context.Context, value any) error
	ValidateCtx(ctx context.Context, value any, tag string, namespace string) error
}

// Rule is custom validation rule/tag.
type Rule struct {
	Tag          string
	Func         validator.Func
	FuncCtx      validator.FuncCtx
	ErrorMsg     string
	ErrorMsgFunc ErrorMsgFunc
}

type ErrorMsgFunc func(fe validator.FieldError) string

type Error struct {
	message string
}

type contextKey string

// wrapper implements Validator interface.
// It is a wrapper around go-playground/validator library.
type wrapper struct {
	validator  *validator.Validate
	translator ut.Translator
}

func New(rules ...Rule) Validator {
	// Create validator and translator
	v := &wrapper{
		// WithRequiredStructEnabled: https://github.com/go-playground/validator/issues/1142
		validator:  validator.New(validator.WithRequiredStructEnabled()),
		translator: ut.New(en.New()).GetFallback(),
	}

	v.registerDefaultErrorMessages()
	v.registerCustomRules()
	v.registerCustomMessages()
	v.registerCustomTypes()

	// Modify fields names in error "namespace".
	v.validator.RegisterTagNameFunc(func(fld reflect.StructField) string {
		// Use "anonymousField" name for anonymous fields, so they can be removed from the error namespace.
		// See "processError" method.
		if fld.Anonymous {
			return anonymousField
		}

		// Prefer name defined by the configKey tag
		name := strings.SplitN(fld.Tag.Get("configKey"), ",", 2)[0]

		// Alternatively use JSON field name in error messages
		if name == "" {
			name = strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		}

		// Alternatively use YAML field name
		if name == "" {
			name = strings.SplitN(fld.Tag.Get("yaml"), ",", 2)[0]
		}

		// As fallback use field name
		if name == "" || name == "-" {
			name = fld.Name
		}

		return name
	})

	// Register extra rules
	v.RegisterRule(rules...)

	return v
}

func (v Error) Error() string {
	return v.message
}

func (v Error) WriteError(w errors.Writer, level int, _ errors.StackTrace) {
	// Disable other formatting
	w.WriteBullet(level)
	w.Write(v.Error())
}

// ValidateValue nested struct fields or slice items.
func (v *wrapper) ValidateValue(value any, tag string) error {
	return v.ValidateCtx(context.Background(), value, tag, "")
}

// Validate nested struct fields or slice items.
func (v *wrapper) Validate(ctx context.Context, value any) error {
	return v.ValidateCtx(ctx, value, "dive", "")
}

// ValidateCtx validates any value.
func (v *wrapper) ValidateCtx(ctx context.Context, value any, tag string, namespace string) error {
	// nolint: errorlint // library always returns validator.ValidationErrors
	if err := v.validator.VarCtx(ctx, value, tag); err != nil {
		return v.processError(err.(validator.ValidationErrors), namespace, reflect.ValueOf(value))
	}
	return nil
}

// RegisterRule by tag and function.
func (v *wrapper) RegisterRule(rules ...Rule) {
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
			panic(errors.New(`please specify validator.Rule.FuncCtx or Func`))
		}

		switch {
		case rule.ErrorMsgFunc != nil:
			v.registerErrorMessageFunc(rule.Tag, rule.ErrorMsgFunc)
		case rule.ErrorMsg != "":
			v.registerErrorMessage(rule.Tag, rule.ErrorMsg)
		default:
			panic(errors.New(`please specify validator.Rule.ErrorMsg or ErrorMsgFunc`))
		}
	}
}

// registerErrorMessage for a tag.
func (v *wrapper) registerErrorMessage(tag, message string) {
	if tag == "" {
		panic(errors.New(`tag cannot be empty`))
	}
	if message == "" {
		panic(errors.New(`message cannot be empty`))
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

// registerErrorMessage for a tag.
func (v *wrapper) registerErrorMessageFunc(tag string, f ErrorMsgFunc) {
	if tag == "" {
		panic(errors.New(`tag cannot be empty`))
	}

	registerFn := func(ut ut.Translator) error {
		return nil
	}
	translationFn := func(ut ut.Translator, fe validator.FieldError) string {
		return f(fe)
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

// processError creates human-readable error message.
func (v *wrapper) processError(err validator.ValidationErrors, namespace string, value reflect.Value) errors.MultiError {
	errs := errors.NewMultiError()
	for _, e := range err {
		// Translate error
		errString := strings.TrimSpace(e.Translate(v.translator))
		if strings.HasPrefix(errString, "Key: ") {
			// Use generic message if the error has not been translated
			errString = fmt.Sprintf("%s is invalid", e.Field())
		}

		// Prefix error with namespace
		errs.Append(&Error{message: prefixErrorWithNamespace(e, errString, namespace, value)})
	}

	return errs
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

	// Is field present at the beginning of the error message? Then remove it.
	errMsgParts := strings.SplitN(errMsg, " ", 2)
	errMsgFirstWord := errMsgParts[0]
	errMsgContainsField := len(namespaceParts) > 0 && namespaceParts[len(namespaceParts)-1] == errMsgFirstWord
	if errMsgContainsField {
		errMsg = errMsgParts[1]
	}

	// Check nil value
	if value.IsValid() {
		// Remove first part, if it is a struct name
		valueType := value.Type()
		for valueType.Kind() == reflect.Ptr {
			// Type to which the pointer refers
			valueType = valueType.Elem()
		}
		if valueType.Kind() == reflect.Struct && namespaceParts[0] == valueType.Name() {
			namespaceParts = namespaceParts[1:]
		}
	}

	// Prepend custom namespace
	if customNamespace != "" {
		namespaceParts = append(strings.Split(customNamespace, "."), namespaceParts...)
	}

	// Prefix error message with namespace
	namespace := strings.Join(namespaceParts, ".")
	switch namespace {
	case "":
		return errMsg
	default:
		return fmt.Sprintf(`"%s" %s`, namespace, errMsg)
	}
}
