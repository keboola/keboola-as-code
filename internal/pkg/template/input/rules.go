package input

import (
	"context"

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

func (r Rules) ValidateValue(userInput interface{}, fieldId string) (err error) {
	if r.Empty() {
		return nil
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
	return validator.ValidateCtx(context.Background(), userInput, string(r), fieldId)
}
