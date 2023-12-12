package input

import (
	goValuate "gopkg.in/Knetic/govaluate.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// If condition, defined by an expression, compiled by https://github.com/Knetic/govaluate/tree/v3.0.0.
// If the result is "false", then Input is hidden in the template use dialog.
type If string

// Evaluate condition.
func (i If) Evaluate(params map[string]any) (bool, error) {
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
		return false, errors.NewNestedError(
			errors.New("cannot evaluate condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}

	return result.(bool), nil
}

func (i If) compile() (*goValuate.EvaluableExpression, error) {
	expression, err := goValuate.NewEvaluableExpression(string(i))
	if err != nil {
		return nil, errors.NewNestedError(
			errors.New("cannot compile condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}
	return expression, nil
}
