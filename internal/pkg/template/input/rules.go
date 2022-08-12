package input

import (
	"context"

	goValidator "github.com/go-playground/validator/v10"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// Rules - validation rules for Input value defined by the author of the template.
type Rules string

// InvalidRulesError is returned if rules definition is invalid.
type InvalidRulesError string

func (e InvalidRulesError) Error() string {
	return string(e)
}

func (r Rules) Empty() bool {
	return len(r) == 0
}

func (r Rules) ValidateValue(input Input, value any) (err error) {
	// Skip empty rules
	if r.Empty() {
		return nil
	}

	// Convert rules to string
	rules := string(r)

	// Handle required object: empty object "{}" is valid for "required" rule, so we have to add custom rule.
	if input.Type == TypeObject {
		rules = "requiredNotEmpty," + rules
	}

	// Catch panic, it means invalid expression.
	defer func() {
		if e := recover(); e != nil {
			msg := cast.ToString(e)
			msg = strhelper.FirstLower(msg)
			msg = regexpcache.MustCompile(` on field '.*'$`).ReplaceAllString(msg, "") // remove "on field ''"
			err = InvalidRulesError(msg)
		}
	}()

	extraRules := []validator.Rule{
		{
			Tag: "requiredNotEmpty",
			Func: func(fl goValidator.FieldLevel) bool {
				return fl.Field().Len() > 0
			},
			ErrorMsg: "{0} is a required field",
		},
	}
	return validator.ValidateCtx(context.Background(), value, rules, input.Id, extraRules...)
}
