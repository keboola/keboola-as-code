package naming

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	SqlExt       = `sql`
	PyExt        = `py`
	JuliaExt     = `jl`
	RExt         = `r`
	TxtExt       = `txt`
	SqlComment   = `--`
	PyComment    = `#`
	JuliaComment = `#`
	RComment     = `#`
	TxtComment   = `//`
)

func CodeFileExt(componentId ComponentId) string {
	switch {
	case componentId.IsSqlTransformation():
		return SqlExt
	case componentId == `keboola.r-transformation`:
		return RExt
	case componentId == `keboola.julia-transformation`:
		return JuliaExt
	case componentId == `keboola.python-spark-transformation`:
		return PyExt
	case componentId == `keboola.python-transformation`:
		return PyExt
	case componentId == `keboola.python-transformation-v2`:
		return PyExt
	case componentId == `keboola.csas-python-transformation-v2`:
		return PyExt
	default:
		return TxtExt
	}
}

func CodeFileComment(ext string) string {
	switch ext {
	case SqlExt:
		return SqlComment
	case RExt:
		return RComment
	case JuliaExt:
		return JuliaComment
	case PyExt:
		return PyComment
	default:
		return TxtComment
	}
}
