package url

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
)

func TestParseQuery(t *testing.T) {
	t.Parallel()

	res, err := ParseQuery("one=two&three=four&five=&six&seven[0]=eight&seven[1]=nine&ten[]=eleven&ten[]=twelve")
	assert.NoError(t, err)

	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	exp.Set("five", "")
	exp.Set("six", "")
	exp.Set("seven", []any{"eight", "nine"})
	exp.Set("ten", []any{"eleven", "twelve"})
	assert.Equal(t, exp, res)
}

func TestParseQuery_Map(t *testing.T) {
	t.Parallel()

	res, err := ParseQuery("one[two]=three&one[four]=five")
	assert.NoError(t, err)

	exp := orderedmap.New()
	exp.Set("one", orderedmap.FromPairs(
		[]orderedmap.Pair{
			{
				Key:   "two",
				Value: "three",
			},
			{
				Key:   "four",
				Value: "five",
			},
		},
	))
	assert.Equal(t, exp, res)
}

func TestParseQuery_Nested(t *testing.T) {
	t.Parallel()

	res, err := ParseQuery("k[x][0]=zero&k[x][2]=one&k[y][0]=two")
	assert.NoError(t, err)

	exp := orderedmap.New()
	exp.Set("k", orderedmap.FromPairs(
		[]orderedmap.Pair{
			{
				Key:   "x",
				Value: []any{"zero", nil, "one"},
			},
			{
				Key:   "y",
				Value: []any{"two"},
			},
		},
	))
	assert.Equal(t, exp, res)
}
