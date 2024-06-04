package preview

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/preview"
)

func TestParseWhereFilter(t *testing.T) {
	t.Parallel()

	type testCase struct {
		input    string
		expected preview.WhereFilter
	}

	cases := []testCase{
		{
			input: "my_column=a",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: keboola.CompareEq,
				Values:   []string{"a"},
			},
		},
		{
			input: "my_column!=a",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: keboola.CompareNe,
				Values:   []string{"a"},
			},
		},
		{
			input: "my_column>=a",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: keboola.CompareGe,
				Values:   []string{"a"},
			},
		},
		{
			input: "my_column<=a",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: keboola.CompareLe,
				Values:   []string{"a"},
			},
		},
		{
			input: "my_column=a,b,c",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: keboola.CompareEq,
				Values:   []string{"a", "b", "c"},
			},
		},
	}

	for _, c := range cases {
		actual, err := ParseWhereFilter(c.input)
		require.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}
}

func TestParseWhereFilter_Errors(t *testing.T) {
	t.Parallel()

	cases := []string{
		"my_column",
		"my_column~",
		"=",
		"my_column=!",
	}

	for _, c := range cases {
		actual, err := ParseWhereFilter(c)
		require.Error(t, err)
		assert.Empty(t, actual)
	}
}

func TestParseColumnOrder(t *testing.T) {
	t.Parallel()

	type testCase struct {
		input    string
		expected preview.ColumnOrder
	}

	cases := []testCase{
		{
			input:    "my_column=asc",
			expected: preview.ColumnOrder{Column: "my_column", Order: keboola.OrderAsc},
		},
		{
			input:    "my_column=desc",
			expected: preview.ColumnOrder{Column: "my_column", Order: keboola.OrderDesc},
		},
		{
			input:    "my_column",
			expected: preview.ColumnOrder{Column: "my_column", Order: keboola.OrderAsc},
		},
	}

	for _, c := range cases {
		actual, err := ParseColumnOrder(c.input)
		require.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}
}
