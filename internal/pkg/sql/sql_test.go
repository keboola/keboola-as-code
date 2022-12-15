package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlSplitAndJoin(t *testing.T) {
	t.Parallel()
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

		// Tests from the UI: https://github.com/keboola/kbc-ui/blob/main/src/scripts/modules/transformations/utils/splitSqlQueries.spec.js
		{
			comment: "split queries",
			input: `
SELECT 1;
Select 2;
SELECT 3;
`,
			output: strings.TrimSpace(`
SELECT 1;

Select 2;

SELECT 3;
`),
			statements: []string{
				"SELECT 1;",
				"Select 2;",
				"SELECT 3;",
			},
		},
		{
			comment: "multi line comments with syntax /* */",
			input: `
SELECT 1;
/*
  Select 2;
*/
SELECT 3;
`,
			output: strings.TrimSpace(`
SELECT 1;

/*
  Select 2;
*/
SELECT 3;
`),
			statements: []string{
				"SELECT 1;",
				"/*\n  Select 2;\n*/\nSELECT 3;",
			},
		},
		{
			comment: "single line comments with -- syntax",
			input: `
SELECT 1;
-- Select 2;
SELECT 3;
`,
			output: strings.TrimSpace(`
SELECT 1;

-- Select 2;
SELECT 3;
`),
			statements: []string{
				"SELECT 1;",
				"-- Select 2;\nSELECT 3;",
			},
		},
		{
			comment: "single line comments with # syntax",
			input: `
SELECT 1;
# Select 2;
SELECT 3;
`,
			output: strings.TrimSpace(`
SELECT 1;

# Select 2;
SELECT 3;
`),
			statements: []string{
				"SELECT 1;",
				"# Select 2;\nSELECT 3;",
			},
		},
		{
			comment: "single line comments with // syntax",
			input: `
SELECT 1;
// Select 2;
SELECT 3;
`,
			output: strings.TrimSpace(`
SELECT 1;

// Select 2;
SELECT 3;
`),
			statements: []string{
				"SELECT 1;",
				"// Select 2;\nSELECT 3;",
			},
		},
		{
			comment: "multi line code with syntax $$",
			input: `
SELECT 1;
execute immediate $$
  SELECT 2;
  SELECT 3;
$$;
`,
			output: strings.TrimSpace(`
SELECT 1;

execute immediate $$
  SELECT 2;
  SELECT 3;
$$;
`),
			statements: []string{
				"SELECT 1;",
				"execute immediate $$\n  SELECT 2;\n  SELECT 3;\n$$;",
			},
		},
	}

	for _, data := range cases {
		assert.Equal(t, data.statements, Split(data.input), "case: "+data.comment)
		assert.Equal(t, data.output, Join(data.statements), "case: "+data.comment)
	}
}
