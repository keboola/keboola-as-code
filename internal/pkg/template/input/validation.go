package input

import (
	"context"
	"fmt"
	"reflect"

	goValidator "github.com/go-playground/validator/v10"
	"github.com/umisama/go-regexpcache"
)

func validateInputId(fl goValidator.FieldLevel) bool {
	return regexpcache.MustCompile(`^[a-zA-Z0-9\.\_]+$`).MatchString(fl.Field().String())
}

// Default value must be of the same type as the Type or Options.
func validateInputDefault(fl goValidator.FieldLevel) bool {
	if fl.Field().Kind() == reflect.Ptr && fl.Field().IsNil() {
		return true
	}
	if fl.Field().IsZero() {
		return true
	}

	// Check if Default is present in Options
	if fl.Parent().FieldByName("Kind").String() == "select" || fl.Parent().FieldByName("Kind").String() == "multiselect" {
		for _, x := range fl.Parent().FieldByName("Options").Interface().([]Option) {
			if string(x) == fl.Field().String() {
				return true
			}
		}
		return false
	}

	// Check that the Default has the right Type for the Kind
	err := validateUserInputTypeByKind(fl.Field(), fl.Field().Kind().String())
	return err == nil
}

// Options must be filled only for select or multiselect Kind.
func validateInputOptions(fl goValidator.FieldLevel) bool {
	if fl.Parent().FieldByName("Kind").String() == "select" || fl.Parent().FieldByName("Kind").String() == "multiselect" {
		return fl.Field().Len() > 0
	}
	return fl.Field().Len() == 0
}

// Valid only for input Kind.
func validateInputType(fl goValidator.FieldLevel) bool {
	return fl.Parent().FieldByName("Kind").String() == "input"
}

// Try to validate Rules with any user input just to check that it does not contain a syntax error and so does not return panic.
func validateInputRules(fl goValidator.FieldLevel) bool {
	if fl.Field().IsZero() {
		return true
	}
	_, panicErr := catchPanicOnRulesValidation(func() error { return validateUserInputWithRules("string", fl.Field().String(), nil) })
	return panicErr == nil
}

// Some input Kinds require specific Type of the input.
func validateUserInputTypeByKind(value interface{}, kind string) error {
	inputType := reflect.TypeOf(value).String()
	switch kind {
	case "password", "textarea":
		if inputType != reflect.String.String() {
			return fmt.Errorf("the input is of %s kind and should be a string, got %s instead", kind, inputType)
		}
	case "confirm":
		if inputType != reflect.Bool.String() {
			return fmt.Errorf("the input is of confirm kind and should be a bool, got %s instead", inputType)
		}
	}
	return nil
}

func validateUserInputWithRules(userInput interface{}, rules string, ctx context.Context) error {
	validate := goValidator.New()
	return validate.VarCtx(ctx, userInput, rules)
}

func catchPanicOnRulesValidation(fn func() error) (err, recovered interface{}) {
	defer func() {
		recovered = recover()
	}()
	return fn(), recovered
}
