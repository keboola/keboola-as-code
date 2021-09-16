package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlSplitAndJoin(t *testing.T) {
	cases := []struct {
		comment    string
		input      string
		output     string
		statements []string
	}{
		{
			comment:    "empty",
			input:      "",
			output:     "",
			statements: []string{},
		},
		{
			comment:    "one statement",
			input:      "SELECT * FROM [bar]",
			output:     "SELECT * FROM [bar]",
			statements: []string{"SELECT * FROM [bar]"},
		},
		{
			comment:    "one statement + whitespaces",
			input:      "   \n\n\nSELECT * FROM [bar]\t\n  ",
			output:     "SELECT * FROM [bar]",
			statements: []string{"SELECT * FROM [bar]"},
		},
		{
			comment: "multiple",
			input:   "   \n\n\nSELECT * FROM [bar];\t\n  INSERT INTO bar VALUES('x', 'y'); TRUNCATE records;;;",
			output:  "SELECT * FROM [bar];\n\nINSERT INTO bar VALUES('x', 'y');\n\nTRUNCATE records;",
			statements: []string{
				"SELECT * FROM [bar];",
				"INSERT INTO bar VALUES('x', 'y');",
				"TRUNCATE records;",
			},
		},
	}

	for _, data := range cases {
		assert.Equal(t, data.statements, Split(data.input), "case: "+data.comment)
		assert.Equal(t, data.output, Join(data.statements), "case: "+data.comment)
	}
}
