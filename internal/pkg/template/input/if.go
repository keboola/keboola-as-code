package input

import (
	"fmt"

	goValuate "gopkg.in/Knetic/govaluate.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// If condition, defined by an expression, compiled by https://github.com/Knetic/govaluate/tree/v3.0.0.
// If the result is "false", then Input is hidden in the template use dialog.
type If string

// Evaluate condition.
func (i If) Evaluate(params map[string]interface{}) (bool, error) {
	// No condition
	if i == "" {
		return true, nil
	}

	// Compile
	expression, err := i.compile()
	if err != nil {
		return false, err
	}

	// Evaluate
	result, err := expression.Evaluate(params)
	if err != nil {
		e := utils.NewMultiError()
		e.Append(fmt.Errorf(`expression: %s`, i))
		e.Append(fmt.Errorf(`error: %w`, err))
		return false, utils.PrefixError(`cannot evaluate condition`, e)
	}

	return result.(bool), nil
}

func (i If) compile() (*goValuate.EvaluableExpression, error) {
	expression, err := goValuate.NewEvaluableExpression(string(i))
	if err != nil {
		e := utils.NewMultiError()
		e.Append(fmt.Errorf(`expression: %s`, i))
		e.Append(fmt.Errorf(`error: %w`, err))
		return nil, utils.PrefixError(`cannot compile condition`, e)
	}
	return expression, nil
}
