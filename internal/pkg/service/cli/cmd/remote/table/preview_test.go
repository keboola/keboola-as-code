package table

import (
	"testing"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/preview"
	"github.com/stretchr/testify/assert"
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
				Operator: "=",
				Values:   []string{"a"},
			},
		},
		{
			input: "my_column!=a",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: "!=",
				Values:   []string{"a"},
			},
		},
		{
			input: "my_column>=a",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: ">=",
				Values:   []string{"a"},
			},
		},
		{
			input: "my_column<=a",
			expected: preview.WhereFilter{
				Column:   "my_column",
				Operator: "<=",
				Values:   []string{"a"},
			},
		},
		{
			input:    "my_column=a,b,c",
			expected: preview.WhereFilter{Column: "my_column", Operator: "=", Values: []string{"a", "b", "c"}},
		},
	}

	for _, c := range cases {
		actual, err := parseWhereFilter(c.input)
		assert.NoError(t, err)
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
		actual, err := parseWhereFilter(c)
		assert.Error(t, err)
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
			expected: preview.ColumnOrder{Column: "my_column", Order: "asc"},
		},
		{
			input:    "my_column=desc",
			expected: preview.ColumnOrder{Column: "my_column", Order: "desc"},
		},
		{
			input:    "my_column",
			expected: preview.ColumnOrder{Column: "my_column", Order: "asc"},
		},
	}

	for _, c := range cases {
		actual, err := parseColumnOrder(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}
}
