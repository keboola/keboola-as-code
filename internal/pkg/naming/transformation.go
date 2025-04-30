package naming

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

const (
	SQLExt       = `sql`
	PyExt        = `py`
	JuliaExt     = `jl`
	RExt         = `r`
	TxtExt       = `txt`
	SQLComment   = `--`
	PyComment    = `#`
	JuliaComment = `#`
	RComment     = `#`
	TxtComment   = `//`
)

func CodeFileExt(componentID keboola.ComponentID) string {
	switch componentID {
	case `keboola.snowflake-transformation`:
		return SQLExt
	case `keboola.synapse-transformation`:
		return SQLExt
	case `keboola.oracle-transformation`:
		return SQLExt
	case `keboola.google-bigquery-transformation`:
		return SQLExt
	case `keboola.teradata-transformation`:
		return SQLExt
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
	case `keboola.python-snowpark-transformation`:
		return PyExt
	case `keboola.python-mlflow-transformation`:
		return PyExt
	default:
		return TxtExt
	}
}

func CodeFileComment(ext string) string {
	switch ext {
	case SQLExt:
		return SQLComment
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
