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
	"strconv"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTranslation "github.com/go-playground/validator/v10/translations/en"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
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
		// See "processError" method.
		if fld.Anonymous {
			return anonymousField
		}

		// Prefer JSON field name in error messages
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]

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

func (v *wrapper) registerCustomRules() {
	v.RegisterRule(
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
			ErrorMsg: "{0} is a required field",
		},
		Rule{
			Tag: "required_not_empty",
			Func: func(fl validator.FieldLevel) bool {
				// Check nil
				if !fl.Field().IsValid() {
					return false
				}
				// Check empty
				return fl.Field().Len() > 0
			},
			ErrorMsg: "{0} is a required field",
		},
		// Alphanumeric string with allowed dash character.
		Rule{
			Tag: "alphanumdash",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				return regexpcache.MustCompile(`^[a-zA-Z0-9\-]+$`).MatchString(fl.Field().String())
			},
			ErrorMsg: "{0} can only contain alphanumeric characters and dash",
		},
		// Template icon
		Rule{
			Tag: "templateicon",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				value := fl.Field().String()
				if strings.HasPrefix(value, "component:") {
					value = strings.TrimPrefix(value, "component:")
					return len(value) > 0
				}
				if strings.HasPrefix(value, "common:") {
					value = strings.TrimPrefix(value, "common:")
					return allowedTemplateIcons[value]
				}
				return false
			},
			ErrorMsg: "{0} does not contain an allowed icon",
		},
		Rule{
			Tag: "mdmax",
			FuncCtx: func(ctx context.Context, fl validator.FieldLevel) bool {
				value := fl.Field().String()
				paramStr := fl.Param()
				param, err := strconv.Atoi(paramStr)
				if err != nil {
					panic(fmt.Sprintf("failed to convert mdmax param \"%v\" to an int", paramStr))
				}
				value = strhelper.StripMarkdown(value)
				return len(value) <= param
			},
			ErrorMsgFunc: func(fe validator.FieldError) string {
				return fmt.Sprintf("%s exceeded maximum length of %s", fe.Tag(), fe.Param())
			},
		},
	)
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
	switch {
	case namespace == "":
		return errMsg
	default:
		return fmt.Sprintf(`"%s" %s`, namespace, errMsg)
	}
}
