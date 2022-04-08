package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodeFileExt(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `sql`, CodeFileExt(`keboola.snowflake-transformation`))
	assert.Equal(t, `py`, CodeFileExt(`keboola.python-transformation-v2`))
}

func TestCodeFileComment(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `--`, CodeFileComment(`sql`))
	assert.Equal(t, `#`, CodeFileComment(`py`))
}
