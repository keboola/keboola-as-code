// Package jsonnet provides Jsonnet functions used by the Stream API import endpoint.
//
// # Jsonnet Functions
//
//	Ip() string - formatted ClientIP address of the client
//	Now() string - current datetime in UTC, in fixed length DefaultTimeFormat, for example "2006-01-01T08:04:05.123Z"
//	Now("%Y-%m-%d") string - current datetime in UTC timezone, in a custom "strftime" compatible format
//	HeaderStr() string - request headers as a string, each lines contains one "Header: value", the lines are sorted alphabetically
//	Header() object - all headers as a JSON object
//	Header("Header-Name") string - value of the header, if it is not found, then an error occurs and the record is not saved
//	Header("Header-Name", "default value") string - value of the header or the default value
//	BodyStr() string - raw request body as a string
//	Body() object - parsed JSON/form-data body as a JSON object
//	Body("some.key1[2].key2") mixed - value of the path in the parsed body, if it is not found, then an error occurs and the record is not saved
//	Body("some.key1[2].key2", "default value") mixed - value of the path in the parsed body or the default value
package jsonnet

import (
	"net/http"

	jsonnetLib "github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/lestrrat-go/strftime"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// DefaultTimeFormat with fixed length, so it can be used for lexicographic sorting in the target database.
	// Value is RFC3339 and ISO8601 compatible.
	DefaultTimeFormat   = "%Y-%m-%dT%H:%M:%S.%fZ"
	ThrowErrOnUndefined = "<<~~errOnUndefined~~>>"
	nowFnName           = "_now"
	headersMapFnName    = "_headersMap"
	headerFnName        = "_header"
	bodyMapFnName       = "_bodyMap"
	bodyPathFnName      = "_bodyPath"
)

func NewPool() *jsonnet.VMPool[recordctx.Context] {
	return jsonnet.NewVMPool(
		func(vm *jsonnet.VM[recordctx.Context]) *jsonnetLib.VM {
			realVM := jsonnetLib.MakeVM()
			realVM.Importer(jsonnet.NewNopImporter())
			registerFunctions(realVM, vm)
			return realVM
		},
	)
}

func Evaluate(vm *jsonnet.VM[recordctx.Context], reqCtx recordctx.Context, template string) (string, error) {
	out, err := vm.Evaluate(template, reqCtx)
	if err != nil {
		var jsonnetErr jsonnetLib.RuntimeError
		if errors.As(err, &jsonnetErr) {
			// Trim "jsonnet error: RUNTIME ERROR: "
			err = errors.Wrap(err, jsonnetErr.Msg)
		}
	}
	return out, err
}

func registerFunctions(realVM *jsonnetLib.VM, vm *jsonnet.VM[recordctx.Context]) {
	// Global functions
	realVM.NativeFunction(ipFn("Ip", vm))
	realVM.NativeFunction(headerStrFn("HeaderStr", vm))
	realVM.NativeFunction(bodyStrFn("BodyStr", vm))
	realVM.Bind("Ip", jsonnet.Alias("Ip"))
	realVM.Bind("HeaderStr", jsonnet.Alias("HeaderStr"))
	realVM.Bind("BodyStr", jsonnet.Alias("BodyStr"))
	realVM.Bind("Now", nowFn())
	realVM.Bind("Header", headerFn())
	realVM.Bind("Body", bodyFn())

	// Internal functions
	// Optional function parameters cannot be specified directly by the Go SDK,
	// so these partial functions are used by global functions above.
	realVM.NativeFunction(nowInternalFn(vm))
	realVM.NativeFunction(headersMapInternalFn(vm))
	realVM.NativeFunction(headerValueInternalFn(vm))
	realVM.NativeFunction(bodyMapInternalFn(vm))
	realVM.NativeFunction(bodyPathInternalFn(vm))
}

func ipFn(fnName string, vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name: fnName,
		Func: func(params []any) (any, error) {
			if len(params) != 0 {
				return nil, errors.Errorf("no parameter expected, found %d", len(params))
			}

			reqCtx := vm.Payload()
			return jsonnet.ValueToJSONType(reqCtx.ClientIP().String()), nil
		},
	}
}

func headerStrFn(fnName string, vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name: fnName,
		Func: func(params []any) (any, error) {
			if len(params) != 0 {
				return nil, errors.Errorf("no parameter expected, found %d", len(params))
			}

			reqCtx := vm.Payload()
			return jsonnet.ValueToJSONType(reqCtx.HeadersString()), nil
		},
	}
}

func bodyStrFn(fnName string, vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name: fnName,
		Func: func(params []any) (any, error) {
			if len(params) != 0 {
				return nil, errors.Errorf("no parameter expected, found %d", len(params))
			}

			body, err := vm.Payload().BodyBytes()
			if err != nil {
				return nil, err
			}

			return jsonnet.ValueToJSONType(body), nil
		},
	}
}

func nowFn() ast.Node {
	formatParam := ast.Identifier("format")
	formatVar := &ast.Var{Id: formatParam}
	defaultFormat := jsonnet.ValueToLiteral(DefaultTimeFormat)

	var node ast.Node = &ast.Function{
		Parameters: []ast.Parameter{{Name: formatParam, DefaultArg: defaultFormat}},
		Body:       applyNativeFn(nowFnName, formatVar),
		NodeBase:   ast.NodeBase{FreeVars: []ast.Identifier{"std"}},
	}

	return node
}

// headerFn - if header == "" then std.native("_headersMap") else std.native("_header", header, defaultValue).
func headerFn() ast.Node {
	headerParam := ast.Identifier("header")
	headerVar := &ast.Var{Id: headerParam}
	defaultValParam := ast.Identifier("default")
	defaultValVar := &ast.Var{Id: defaultValParam}
	emptyStr := jsonnet.ValueToLiteral("")
	throwErrOnUndefined := jsonnet.ValueToLiteral(ThrowErrOnUndefined)
	var node ast.Node = &ast.Function{
		Parameters: []ast.Parameter{{Name: headerParam, DefaultArg: emptyStr}, {Name: defaultValParam, DefaultArg: throwErrOnUndefined}},
		Body: &ast.Conditional{
			Cond:        &ast.Binary{Right: headerVar, Left: emptyStr, Op: ast.BopManifestEqual},
			BranchTrue:  applyNativeFn(headersMapFnName),
			BranchFalse: applyNativeFn(headerFnName, headerVar, defaultValVar),
		},
		NodeBase: ast.NodeBase{FreeVars: []ast.Identifier{"std"}},
	}

	return node
}

// bodyFn - if path == "" then std.native("_bodyMap") else std.native("_bodyPath", path, defaultValue).
func bodyFn() ast.Node {
	pathParam := ast.Identifier("path")
	pathVar := &ast.Var{Id: pathParam}
	defaultValParam := ast.Identifier("default")
	defaultValVar := &ast.Var{Id: defaultValParam}
	emptyStr := jsonnet.ValueToLiteral("")
	throwErrOnUndefined := jsonnet.ValueToLiteral(ThrowErrOnUndefined)
	var node ast.Node = &ast.Function{
		Parameters: []ast.Parameter{{Name: pathParam, DefaultArg: emptyStr}, {Name: defaultValParam, DefaultArg: throwErrOnUndefined}},
		Body: &ast.Conditional{
			Cond:        &ast.Binary{Right: &ast.Var{Id: pathParam}, Left: emptyStr, Op: ast.BopManifestEqual},
			BranchTrue:  applyNativeFn(bodyMapFnName),
			BranchFalse: applyNativeFn(bodyPathFnName, pathVar, defaultValVar),
		},
		NodeBase: ast.NodeBase{FreeVars: []ast.Identifier{"std"}},
	}

	return node
}

func nowInternalFn(vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   nowFnName,
		Params: ast.Identifiers{"format"},
		Func: func(params []any) (any, error) {
			if len(params) != 1 {
				return nil, errors.Errorf("one parameter expected, found %d", len(params))
			}

			format, ok := params[0].(string)
			if !ok {
				return nil, errors.New("parameter must be a string")
			}

			formatter, err := strftime.New(format, strftime.WithMilliseconds('f'))
			if err != nil {
				return nil, errors.Errorf(`datetime format "%s" is invalid: %w`, format, err)
			}

			reqCtx := vm.Payload()
			return jsonnet.ValueToJSONType(formatter.FormatString(reqCtx.Timestamp().UTC())), nil
		},
	}
}

func headersMapInternalFn(vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name: headersMapFnName,
		Func: func(params []any) (any, error) {
			if len(params) != 0 {
				return nil, errors.Errorf("no parameter expected, found %d", len(params))
			}

			reqCtx := vm.Payload()
			return jsonnet.ValueToJSONType(reqCtx.HeadersMap()), nil
		},
	}
}

func headerValueInternalFn(vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   headerFnName,
		Params: ast.Identifiers{"path", "default"},
		Func: func(params []any) (any, error) {
			if len(params) != 2 {
				return nil, errors.Errorf("two parameters expected, found %d", len(params))
			}

			name, ok := params[0].(string)
			defaultVal := params[1]
			if !ok {
				return nil, errors.New("parameter must be a string")
			}

			reqCtx := vm.Payload()
			value, found := reqCtx.HeadersMap().Get(http.CanonicalHeaderKey(name))
			if !found {
				if defaultVal == ThrowErrOnUndefined {
					return nil, errors.Errorf(`header "%s" not found`, http.CanonicalHeaderKey(name))
				} else {
					return defaultVal, nil
				}
			}
			return jsonnet.ValueToJSONType(value), nil
		},
	}
}

func bodyMapInternalFn(vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name: bodyMapFnName,
		Func: func(params []any) (any, error) {
			if len(params) != 0 {
				return nil, errors.Errorf("no parameter expected, found %d", len(params))
			}

			reqCtx := vm.Payload()
			bodyMap, err := reqCtx.BodyMap()
			if err != nil {
				return nil, err
			}
			return jsonnet.ValueToJSONType(bodyMap), nil
		},
	}
}

func bodyPathInternalFn(vm *jsonnet.VM[recordctx.Context]) *jsonnet.NativeFunction {
	return &jsonnet.NativeFunction{
		Name:   bodyPathFnName,
		Params: ast.Identifiers{"path", "default"},
		Func: func(params []any) (any, error) {
			if len(params) != 2 {
				return nil, errors.Errorf("two parameters expected, found %d", len(params))
			}

			path, ok := params[0].(string)
			defaultVal := params[1]
			if !ok {
				return nil, errors.New("first parameter must be a string")
			}

			reqCtx := vm.Payload()
			bodyMap, err := reqCtx.BodyMap()
			if err != nil {
				return nil, err
			}

			val, _, err := bodyMap.GetNested(path)
			if err != nil {
				if defaultVal == ThrowErrOnUndefined {
					return nil, errors.Errorf(`path "%s" not found in the body`, path)
				} else {
					return defaultVal, nil
				}
			}
			return jsonnet.ValueToJSONType(val), nil
		},
	}
}

func applyNativeFn(fnName string, args ...ast.Node) ast.Node {
	var freeVars []ast.Identifier
	var fnArgs []ast.CommaSeparatedExpr
	for _, item := range args {
		fnArgs = append(fnArgs, ast.CommaSeparatedExpr{Expr: item})
		// Build list of the freeVars manually, so we can skip parser.PreprocessAst step, it is faster
		if v, ok := item.(*ast.Var); ok {
			freeVars = append(freeVars, v.Id)
		}
	}

	return &ast.Apply{
		NodeBase:  ast.NodeBase{FreeVars: freeVars},
		Target:    jsonnet.Alias(fnName),
		Arguments: ast.Arguments{Positional: fnArgs},
	}
}
