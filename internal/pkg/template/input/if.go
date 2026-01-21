package input

import (
	"regexp"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// If condition, defined by an expression, compiled by https://github.com/expr-lang/expr.
// If the result is "false", then Input is hidden in the template use dialog.
type If string

// varRefRegex matches govaluate-style variable references like [variable-name].
// In govaluate, [variable-name] was used to reference variables with special characters.
// In expr, we convert this to a function call get("variable-name") for safe access.
var varRefRegex = regexp.MustCompile(`\[([a-zA-Z0-9_-]+)\]`)

// convertGovaluateToExpr converts govaluate-style expressions to expr-compatible syntax.
// Specifically, it converts [variable-name] to get("variable-name") function calls.
func convertGovaluateToExpr(expression string) string {
	return varRefRegex.ReplaceAllString(expression, `get("$1")`)
}

// Evaluate condition.
func (i If) Evaluate(params map[string]any) (bool, error) {
	// No condition
	if i == "" {
		return true, nil
	}

	// Convert govaluate syntax to expr syntax
	exprStr := convertGovaluateToExpr(string(i))

	// Create a custom "get" function that retrieves parameters and returns an error if not found
	var missingParam string
	getFunc := func(name string) any {
		if val, ok := params[name]; ok {
			return val
		}
		missingParam = name
		return nil
	}

	// Create environment with the get function
	env := map[string]any{
		"get": getFunc,
	}

	// Compile with environment for type inference
	program, err := expr.Compile(exprStr, expr.Env(env), expr.AsBool())
	if err != nil {
		return false, errors.NewNestedError(
			errors.New("cannot compile condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}

	// Evaluate
	result, err := expr.Run(program, env)
	if err != nil {
		return false, errors.NewNestedError(
			errors.New("cannot evaluate condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}

	// Check if a parameter was missing during evaluation
	if missingParam != "" {
		return false, errors.NewNestedError(
			errors.New("cannot evaluate condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: No parameter '%s' found.", missingParam),
		)
	}

	// Handle nil result (shouldn't happen with AsBool(), but be safe)
	if result == nil {
		return false, nil
	}

	return result.(bool), nil
}

// compile validates the expression syntax without environment type inference.
// Used for validation purposes where we only need to check if the expression is syntactically valid.
func (i If) compile() (*vm.Program, error) {
	// Convert govaluate syntax to expr syntax
	exprStr := convertGovaluateToExpr(string(i))

	// For validation, we need to provide a mock environment structure
	// with a get function that accepts any string
	env := map[string]any{
		"get": func(name string) any { return nil },
	}

	program, err := expr.Compile(exprStr, expr.Env(env), expr.AsBool())
	if err != nil {
		return nil, errors.NewNestedError(
			errors.New("cannot compile condition"),
			errors.Errorf("expression: %s", i),
			errors.Errorf("error: %w", err),
		)
	}
	return program, nil
}
