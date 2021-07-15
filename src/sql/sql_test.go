package sql

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
			output:     "   \n\n\nSELECT * FROM [bar]\t\n  ",
			statements: []string{"   \n\n\nSELECT * FROM [bar]\t\n  "},
		},
		{
			comment: "multiple",
			input:   "   \n\n\nSELECT * FROM [bar];\t\n  INSERT INTO bar VALUES('x', 'y'); TRUNCATE records;;;",
			output:  "   \n\n\nSELECT * FROM [bar];\n\n\t\n  INSERT INTO bar VALUES('x', 'y');\n\n TRUNCATE records;",
			statements: []string{
				"   \n\n\nSELECT * FROM [bar];",
				"\t\n  INSERT INTO bar VALUES('x', 'y');",
				" TRUNCATE records;",
			},
		},
	}

	for _, data := range cases {
		assert.Equal(t, data.statements, Split(data.input), "case: "+data.comment)
		assert.Equal(t, data.output, Join(data.statements), "case: "+data.comment)
	}
}
