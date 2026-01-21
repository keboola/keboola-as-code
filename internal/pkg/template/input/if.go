package input

import (
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// If condition, defined by an expression, compiled by https://github.com/expr-lang/expr.
// If the result is "false", then Input is hidden in the template use dialog.
type If string

// Evaluate condition.
func (i If) Evaluate(params map[string]any) (bool, error) {
	// No condition
	if i == "" {
		return true, nil
	}

	// Compile with environment for type inference
	program, err := i.compileWithEnv(params)
	if err != nil {
		return false, err
	}

	// Evaluate
	result, err := expr.Run(program, params)
	if err != nil {
		return false, errors.NewNestedError(
			errors.New("cannot evaluate condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}

	return result.(bool), nil
}

// compile validates the expression syntax without environment type inference.
// Used for validation purposes where we only need to check if the expression is syntactically valid.
func (i If) compile() (*vm.Program, error) {
	program, err := expr.Compile(string(i), expr.AsBool())
	if err != nil {
		return nil, errors.NewNestedError(
			errors.New("cannot compile condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}
	return program, nil
}

// compileWithEnv compiles the expression with environment for type inference.
// Used when evaluating the expression with actual parameter values.
func (i If) compileWithEnv(env map[string]any) (*vm.Program, error) {
	program, err := expr.Compile(string(i), expr.Env(env), expr.AsBool())
	if err != nil {
		return nil, errors.NewNestedError(
			errors.New("cannot compile condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}
	return program, nil
}
