package url

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
)

func TestParseQuery(t *testing.T) {
	t.Parallel()

	res, err := ParseQuery("one=two&three=four&five=&six&seven[]=eight&seven[]=nine")
	assert.NoError(t, err)

	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	exp.Set("five", "")
	exp.Set("six", "")
	exp.Set("seven[]", []any{"eight", "nine"})
	assert.Equal(t, exp, res)
}
