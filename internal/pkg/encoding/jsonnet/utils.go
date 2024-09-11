package jsonnet

import (
	"unsafe"

	"github.com/google/go-jsonnet/ast"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
)

// ValueToLiteral converts Go value to jsonnet.Ast literal.
func ValueToLiteral(v any) ast.Node {
	if v == nil {
		return &ast.LiteralNull{}
	}

	switch v := v.(type) {
	case bool:
		return &ast.LiteralBoolean{Value: v}
	case int:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case int32:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case int64:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case float32:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case float64:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case []any:
		elements := make([]ast.CommaSeparatedExpr, 0)
		for _, aVal := range v {
			elements = append(elements, ast.CommaSeparatedExpr{Expr: ValueToLiteral(aVal)})
		}
		return &ast.Array{Elements: elements}
	case map[string]any:
		fields := make(ast.DesugaredObjectFields, 0)
		for mKey, mVal := range v {
			fields = append(fields, ast.DesugaredObjectField{
				Hide: ast.ObjectFieldInherit,
				Name: ValueToLiteral(mKey),
				Body: ValueToLiteral(mVal),
			})
		}
		return &ast.DesugaredObject{Fields: fields}
	default:
		return &ast.LiteralString{Value: cast.ToString(v), Kind: ast.StringDouble}
	}
}

// ValueToJSONType converts Go value to a Json value for the Jsonnet VM.
func ValueToJSONType(in any) any {
	switch v := in.(type) {
	case []byte:
		return bytesToStr(v)
	case *orderedmap.OrderedMap:
		m := make(map[string]any)
		for _, k := range v.Keys() {
			m[k], _ = v.Get(k)
			m[k] = ValueToJSONType(m[k])
		}
		return m
	case []any:
		for i, arrVal := range v {
			v[i] = ValueToJSONType(arrVal)
		}
		return v
	}

	return in
}

func bytesToStr(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
