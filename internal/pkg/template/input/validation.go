package input

import (
	"context"
	"fmt"
	"reflect"

	goValidator "github.com/go-playground/validator/v10"
	"github.com/umisama/go-regexpcache"
	goValuate "gopkg.in/Knetic/govaluate.v3"
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
	if fl.Parent().FieldByName("Kind").String() == KindSelect {
		return findValueInOptions(fl.Field().String(), fl)
	}

	if fl.Parent().FieldByName("Kind").String() == KindMultiSelect {
		defaultValues := fl.Field().Interface().([]string)
		for _, defaultValue := range defaultValues {
			found := findValueInOptions(defaultValue, fl)
			if !found {
				return false
			}
		}
		return true
	}

	if fl.Parent().FieldByName("Kind").String() == KindInput && fl.Parent().FieldByName("Type").String() != "" {
		err := validateUserInputByType(fl.Field(), fl.Parent().FieldByName("Type").String())
		if err != nil {
			return false
		}
	}

	// Check that the Default has the right Type for the Kind
	err := validateUserInputTypeByKind(fl.Field(), fl.Parent().FieldByName("Kind").String())
	return err == nil
}

func findValueInOptions(value string, fl goValidator.FieldLevel) bool {
	for _, option := range fl.Parent().FieldByName("Options").Interface().([]Option) {
		if option.Id == value {
			return true
		}
	}
	return false
}

// Options must be filled only for select or multiselect Kind.
func validateInputOptions(fl goValidator.FieldLevel) bool {
	if fl.Parent().FieldByName("Kind").String() == KindSelect || fl.Parent().FieldByName("Kind").String() == KindMultiSelect {
		return fl.Field().Len() > 0
	}
	return fl.Field().Len() == 0
}

// Valid only for input Kind.
func validateInputType(fl goValidator.FieldLevel) bool {
	return fl.Parent().FieldByName("Kind").String() == KindInput
}

// Try to validate Rules with any user input just to check that it does not contain a syntax error and so does not return panic.
func validateInputRules(fl goValidator.FieldLevel) bool {
	if fl.Field().IsZero() {
		return true
	}
	_, panicErr := catchPanicOnRulesValidation(func() error { return validateUserInputWithRules("string", fl.Field().String(), nil) })
	return panicErr == nil
}

// Validate If definition.
func validateInputIf(fl goValidator.FieldLevel) bool {
	if fl.Field().IsZero() {
		return true
	}
	_, err := goValuate.NewEvaluableExpression(fl.Field().String())
	return err == nil
}

// Some input Kinds require specific Type of the input.
func validateUserInputTypeByKind(value interface{}, kind string) error {
	inputType := reflect.TypeOf(value).String()
	switch kind {
	case KindPassword, KindTextarea:
		if inputType != reflect.String.String() {
			return fmt.Errorf("the input is of %s kind and should be a string, got %s instead", kind, inputType)
		}
	case KindConfirm:
		if inputType != reflect.Bool.String() {
			return fmt.Errorf("the input is of confirm kind and should be a bool, got %s instead", inputType)
		}
	}
	return nil
}

func validateUserInputByType(userInput interface{}, inputType string) error {
	switch inputType {
	case reflect.Float64.String():
		return validateUserInputByReflectKind(userInput, reflect.Float64)
	case reflect.Int.String():
		return validateUserInputByReflectKind(userInput, reflect.Int)
	case reflect.String.String():
		return validateUserInputByReflectKind(userInput, reflect.String)
	default:
		panic(fmt.Errorf("unknown type %s", inputType))
	}
}

func validateUserInputByReflectKind(userInput interface{}, expectedType reflect.Kind) error {
	userInputIsValue := reflect.TypeOf(userInput).String() == "reflect.Value"
	if userInputIsValue {
		userInputValue := userInput.(reflect.Value)
		if userInputValue.Kind() != expectedType {
			return fmt.Errorf(
				"the input should be a type %s, got %s instead",
				expectedType.String(),
				userInputValue.Kind().String(),
			)
		}
		return nil
	}

	if reflect.TypeOf(userInput).Kind() != expectedType {
		return fmt.Errorf(
			"the input should be a type %s, got %s instead",
			expectedType.String(),
			reflect.TypeOf(userInput).Kind().String(),
		)
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
