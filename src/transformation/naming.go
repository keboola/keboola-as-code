package transformation

import (
	"fmt"
	"keboola-as-code/src/utils"
)

const (
	blocksDir         = `blocks`
	blockNameTemplate = utils.PathTemplate(`{block_order}-{block_name}`)
	codeNameTemplate  = utils.PathTemplate(`{code_order}-{code_name}`)
)

func blockPath(index int, name string) string {
	return utils.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, index+1),
		"block_name":  utils.NormalizeName(name),
	})
}

func codePath(index int, name string) string {
	return utils.ReplacePlaceholders(string(codeNameTemplate), map[string]interface{}{
		"code_order": fmt.Sprintf(`%03d`, index+1),
		"code_name":  utils.NormalizeName(name),
	})
}

func codeFileExt(componentId string) string {
	switch componentId {
	case `keboola.snowflake-transformation`:
		return `sql`
	case `keboola.synapse-transformation`:
		return `sql`
	case `keboola.oracle-transformation`:
		return `sql`
	case `keboola.r-transformation`:
		return `r`
	case `keboola.julia-transformation`:
		return `jl`
	case `keboola.python-spark-transformation`:
		return `py`
	case `keboola.python-transformation`:
		return `py`
	case `keboola.python-transformation-v2`:
		return `py`
	case `keboola.csas-python-transformation-v2`:
		return `py`
	default:
		return `txt`
	}
}
