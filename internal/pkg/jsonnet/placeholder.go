package jsonnet

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/google/go-jsonnet/ast"
	"github.com/google/go-jsonnet/parser"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
)

const (
	placeholderStart = "<<~~func:"
	placeholderEnd   = "~~>>"
	configIdFunc     = "ConfigId"
	configRowIdFunc  = "ConfigRowId"
	inputFunc        = "Input"
)

// nolint: gochecknoglobals
var placeholderRegexp = regexpcache.MustCompile(fmt.Sprintf("%s([^:]+):([^~]+)%s",
	regexp.QuoteMeta(placeholderStart),
	regexp.QuoteMeta(placeholderEnd),
))

// ConfigIdPlaceholder generates <<~~func:ConfigId:["<ID>"]~~>>.
func ConfigIdPlaceholder(id string) string {
	return functionCallPlaceholder(configIdFunc, id)
}

// ConfigRowIdPlaceholder generates <<~~func:ConfigRowId:["<ID>"]~~>>.
func ConfigRowIdPlaceholder(id string) string {
	return functionCallPlaceholder(configRowIdFunc, id)
}

// InputPlaceholder generates <<~~func:Input:["<InputID>"]~~>>.
func InputPlaceholder(inputId string) string {
	return functionCallPlaceholder(inputFunc, inputId)
}

// functionCallPlaceholder generates <<~~func:<FUNC_NAME>>:["<ARG1>","<ARG2>"]~~>>.
func functionCallPlaceholder(funcName string, args ...interface{}) string {
	return fmt.Sprintf(`%s%s:%s%s`, placeholderStart, funcName, json.MustEncode(args, false), placeholderEnd)
}

func ReplacePlaceholdersRecursive(node ast.Node) {
	VisitAst(&node, func(nodePtr *ast.Node) {
		if v, ok := (*nodePtr).(*ast.LiteralString); ok {
			*nodePtr = ReplacePlaceholders(v)
		}
	})
}

// ReplacePlaceholders in a string AST node.
// Example input:  "before <<~~func:ConfigId:["my-id"]~~>> middle <<~~func:FuncName:["foo", "bar", 123]~~>> end"
// Example output: "before " + ConfigId("my-id") + " middle " + FuncName("foo", "bar", 123) + " end".
func ReplacePlaceholders(node *ast.LiteralString) ast.Node {
	// Unescape if needed
	unescaped := node.Value
	if node.Kind.FullyEscaped() {
		if v, err := parser.StringUnescape(node.Loc(), node.Value); err == nil {
			unescaped = v
		}
	}

	// No placeholder, bypass
	if !strings.Contains(unescaped, placeholderStart) {
		return node
	}

	// Replace placeholders with function call
	replaceCallback := func(funcName string, args []interface{}) ast.Node {
		// Arguments definition, map args -> funcArgs
		var funcArgs []ast.CommaSeparatedExpr
		for _, value := range args {
			funcArgs = append(funcArgs, ast.CommaSeparatedExpr{
				Expr: ValueToLiteral(value),
			})
		}

		// Function call definition: FuncName(funcArgs...)
		return &ast.Apply{
			Target:    &ast.Var{Id: ast.Identifier(funcName)},
			Arguments: ast.Arguments{Positional: funcArgs},
		}
	}

	// Concat nodes with + operation
	return concatWithBinaryOp(splitPlaceholders(unescaped, replaceCallback), ast.BopPlus)
}

// splitPlaceholders from str, see ReplacePlaceholders.
func splitPlaceholders(str string, replace func(funcName string, args []interface{}) ast.Node) []ast.Node {
	matches := placeholderRegexp.FindAllStringSubmatchIndex(str, -1)

	output := make([]ast.Node, 0)
	prevEnd := 0
	for _, indices := range matches {
		// indices: (0)start, (1)end, (2-3)group1, (4-5)group2
		before := str[prevEnd:indices[0]]
		if len(before) > 0 {
			output = append(output, ValueToLiteral(before))
		}

		funcName := str[indices[2]:indices[3]]
		var args []interface{}
		json.MustDecodeString(str[indices[4]:indices[5]], &args)
		output = append(output, replace(funcName, args))

		prevEnd = indices[1]
	}

	if lastPart := str[prevEnd:]; len(lastPart) > 0 {
		output = append(output, ValueToLiteral(lastPart))
	}

	return output
}

// concatWithBinaryOp joins nodes with a binary operation.
// For example: nodes[0] <OP> nodes[1] <OP> nodes[2].
func concatWithBinaryOp(nodes []ast.Node, op ast.BinaryOp) ast.Node {
	var output ast.Node
	for _, node := range nodes {
		// Fist node
		if output == nil {
			output = node
			continue
		}

		// Other nodes
		output = &ast.Binary{
			Op:    op,
			Left:  output,
			Right: node,
		}
	}
	return output
}

// StripIdPlaceholder converts ConfigId/ConfigRowId placeholder to the value.
// It is used to remove placeholders from objects paths,
// if {config_id} or {config_row_id} is used in the naming template.
// Example input: <<~~func:ConfigId:["my-id"]~~>>
// Example output: my-id.
func StripIdPlaceholder(str string) string {
	funcName, args, found := parsePlaceholder(str)

	// ConfigId/ConfigRowId function and one string argument - ID
	if found && len(args) == 1 && (funcName == configIdFunc || funcName == configRowIdFunc) {
		if id, ok := args[0].(string); ok {
			return id
		}
	}
	return str
}

func parsePlaceholder(str string) (funcName string, args []interface{}, found bool) {
	if !isPlaceholder(str) {
		return "", nil, false
	}

	// Trim prefix and suffix
	str = strings.TrimPrefix(str, placeholderStart)
	str = strings.TrimSuffix(str, placeholderEnd)

	// Split to funcName and args
	parts := strings.SplitN(str, ":", 2)
	if len(parts) < 2 {
		return "", nil, false
	}
	funcName = parts[0]
	json.MustDecodeString(parts[1], &args)
	return funcName, args, true
}

func isPlaceholder(str string) bool {
	return strings.HasPrefix(str, placeholderStart) && strings.HasSuffix(str, placeholderEnd)
}
