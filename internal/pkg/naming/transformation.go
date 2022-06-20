package naming

import (
	"github.com/keboola/go-client/pkg/storageapi"
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

func CodeFileExt(componentId storageapi.ComponentID) string {
	switch componentId {
	case `keboola.snowflake-transformation`:
		return SqlExt
	case `keboola.synapse-transformation`:
		return SqlExt
	case `keboola.oracle-transformation`:
		return SqlExt
	case `keboola.r-transformation`:
		return RExt
	case `keboola.julia-transformation`:
		return JuliaExt
	case `keboola.python-spark-transformation`:
		return PyExt
	case `keboola.python-transformation`:
		return PyExt
	case `keboola.python-transformation-v2`:
		return PyExt
	case `keboola.csas-python-transformation-v2`:
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
