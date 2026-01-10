package blockcode

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestGetDelimiter_SQL(t *testing.T) {
	t.Parallel()

	d := GetDelimiter(keboola.ComponentID("keboola.snowflake-transformation"))
	assert.Equal(t, "/* ", d.Start)
	assert.Equal(t, " */", d.End)
	assert.Equal(t, ";", d.Stmt)
	assert.Equal(t, []string{"--", "//"}, d.Inline)
}

func TestGetDelimiter_Python(t *testing.T) {
	t.Parallel()

	d := GetDelimiter(keboola.ComponentID("keboola.python-transformation-v2"))
	assert.Equal(t, "# ", d.Start)
	assert.Equal(t, "", d.End)
	assert.Equal(t, "", d.Stmt)
	assert.Equal(t, []string{"#"}, d.Inline)
}

func TestGetDelimiter_Unknown(t *testing.T) {
	t.Parallel()

	d := GetDelimiter(keboola.ComponentID("unknown.component"))
	assert.Equal(t, "/* ", d.Start)
	assert.Equal(t, " */", d.End)
	assert.Equal(t, "", d.Stmt)
}

func TestBlocksToString_SQL(t *testing.T) {
	t.Parallel()

	blocks := []*model.Block{
		{
			Name: "Data Prep",
			Codes: []*model.Code{
				{
					Name:    "Load",
					Scripts: model.Scripts{model.StaticScript{Value: "SELECT * FROM source"}},
				},
				{
					Name:    "Clean",
					Scripts: model.Scripts{model.StaticScript{Value: "DELETE FROM target WHERE id IS NULL"}},
				},
			},
		},
		{
			Name: "Transform",
			Codes: []*model.Code{
				{
					Name:    "Aggregate",
					Scripts: model.Scripts{model.StaticScript{Value: "INSERT INTO result SELECT COUNT(*) FROM source"}},
				},
			},
		},
	}

	result := BlocksToString(blocks, keboola.ComponentID("keboola.snowflake-transformation"), nil)

	expected := `/* ===== BLOCK: Data Prep ===== */

/* ===== CODE: Load ===== */
SELECT * FROM source;

/* ===== CODE: Clean ===== */
DELETE FROM target WHERE id IS NULL;

/* ===== BLOCK: Transform ===== */

/* ===== CODE: Aggregate ===== */
INSERT INTO result SELECT COUNT(*) FROM source;`

	assert.Equal(t, expected, result)
}

func TestBlocksToString_Python(t *testing.T) {
	t.Parallel()

	blocks := []*model.Block{
		{
			Name: "Data Processing",
			Codes: []*model.Code{
				{
					Name:    "Load Data",
					Scripts: model.Scripts{model.StaticScript{Value: "import pandas as pd\ndf = pd.read_csv('/data/in/tables/source.csv')"}},
				},
				{
					Name:    "Transform",
					Scripts: model.Scripts{model.StaticScript{Value: "df['new_col'] = df['col1'] * 2"}},
				},
			},
		},
	}

	result := BlocksToString(blocks, keboola.ComponentID("keboola.python-transformation-v2"), nil)

	expected := `# ===== BLOCK: Data Processing =====

# ===== CODE: Load Data =====
import pandas as pd
df = pd.read_csv('/data/in/tables/source.csv')

# ===== CODE: Transform =====
df['new_col'] = df['col1'] * 2`

	assert.Equal(t, expected, result)
}

func TestParseString_SQL(t *testing.T) {
	t.Parallel()

	content := `/* ===== BLOCK: Data Prep ===== */

/* ===== CODE: Load ===== */
SELECT * FROM source;

/* ===== CODE: Clean ===== */
DELETE FROM target WHERE id IS NULL;

/* ===== BLOCK: Transform ===== */

/* ===== CODE: Aggregate ===== */
INSERT INTO result SELECT COUNT(*) FROM source;`

	blocks := ParseString(content, keboola.ComponentID("keboola.snowflake-transformation"))

	assert.Len(t, blocks, 2)

	assert.Equal(t, "Data Prep", blocks[0].Name)
	assert.Len(t, blocks[0].Codes, 2)
	assert.Equal(t, "Load", blocks[0].Codes[0].Name)
	assert.Equal(t, "SELECT * FROM source;", blocks[0].Codes[0].Script)
	assert.Equal(t, "Clean", blocks[0].Codes[1].Name)

	assert.Equal(t, "Transform", blocks[1].Name)
	assert.Len(t, blocks[1].Codes, 1)
	assert.Equal(t, "Aggregate", blocks[1].Codes[0].Name)
}

func TestParseString_Python(t *testing.T) {
	t.Parallel()

	content := `# ===== BLOCK: Data Processing =====

# ===== CODE: Load Data =====
import pandas as pd
df = pd.read_csv('/data/in/tables/source.csv')

# ===== CODE: Transform =====
df['new_col'] = df['col1'] * 2`

	blocks := ParseString(content, keboola.ComponentID("keboola.python-transformation-v2"))

	assert.Len(t, blocks, 1)
	assert.Equal(t, "Data Processing", blocks[0].Name)
	assert.Len(t, blocks[0].Codes, 2)
	assert.Equal(t, "Load Data", blocks[0].Codes[0].Name)
	assert.Contains(t, blocks[0].Codes[0].Script, "import pandas")
	assert.Equal(t, "Transform", blocks[0].Codes[1].Name)
}

func TestParseString_NoBlocks(t *testing.T) {
	t.Parallel()

	content := `SELECT * FROM my_table;`

	blocks := ParseString(content, keboola.ComponentID("keboola.snowflake-transformation"))

	assert.Len(t, blocks, 1)
	assert.Equal(t, "New Code Block", blocks[0].Name)
	assert.Len(t, blocks[0].Codes, 1)
	assert.Equal(t, "New Code", blocks[0].Codes[0].Name)
	assert.Equal(t, "SELECT * FROM my_table;", blocks[0].Codes[0].Script)
}

func TestParseString_SharedCode(t *testing.T) {
	t.Parallel()

	content := `/* ===== BLOCK: My Block ===== */

/* ===== SHARED CODE: Helper Functions ===== */
-- shared code content here

/* ===== CODE: Main ===== */
SELECT * FROM table;`

	blocks := ParseString(content, keboola.ComponentID("keboola.snowflake-transformation"))

	assert.Len(t, blocks, 1)
	assert.Len(t, blocks[0].Codes, 2)
	assert.True(t, blocks[0].Codes[0].IsShared)
	assert.Equal(t, "Helper Functions", blocks[0].Codes[0].Name)
	assert.False(t, blocks[0].Codes[1].IsShared)
	assert.Equal(t, "Main", blocks[0].Codes[1].Name)
}

func TestRoundTrip_SQL(t *testing.T) {
	t.Parallel()

	componentID := keboola.ComponentID("keboola.snowflake-transformation")

	// Create original blocks
	originalBlocks := []*model.Block{
		{
			Name: "Block One",
			Codes: []*model.Code{
				{
					Name:    "Code A",
					Scripts: model.Scripts{model.StaticScript{Value: "SELECT 1"}},
				},
			},
		},
	}

	// Convert to string
	str := BlocksToString(originalBlocks, componentID, nil)

	// Parse back
	parsed := ParseString(str, componentID)

	// Verify
	assert.Len(t, parsed, 1)
	assert.Equal(t, "Block One", parsed[0].Name)
	assert.Len(t, parsed[0].Codes, 1)
	assert.Equal(t, "Code A", parsed[0].Codes[0].Name)
	assert.Contains(t, parsed[0].Codes[0].Script, "SELECT 1")
}

func TestRoundTrip_Python(t *testing.T) {
	t.Parallel()

	componentID := keboola.ComponentID("keboola.python-transformation-v2")

	// Create original blocks
	originalBlocks := []*model.Block{
		{
			Name: "Processing",
			Codes: []*model.Code{
				{
					Name:    "Main",
					Scripts: model.Scripts{model.StaticScript{Value: "print('hello')"}},
				},
			},
		},
	}

	// Convert to string
	str := BlocksToString(originalBlocks, componentID, nil)

	// Parse back
	parsed := ParseString(str, componentID)

	// Verify
	assert.Len(t, parsed, 1)
	assert.Equal(t, "Processing", parsed[0].Name)
	assert.Len(t, parsed[0].Codes, 1)
	assert.Equal(t, "Main", parsed[0].Codes[0].Name)
	assert.Equal(t, "print('hello')", parsed[0].Codes[0].Script)
}

func TestIsSQLComponent(t *testing.T) {
	t.Parallel()

	assert.True(t, IsSQLComponent(keboola.ComponentID("keboola.snowflake-transformation")))
	assert.True(t, IsSQLComponent(keboola.ComponentID("keboola.synapse-transformation")))
	assert.False(t, IsSQLComponent(keboola.ComponentID("keboola.python-transformation-v2")))
}

func TestIsScriptComponent(t *testing.T) {
	t.Parallel()

	assert.True(t, IsScriptComponent(keboola.ComponentID("keboola.python-transformation-v2")))
	assert.True(t, IsScriptComponent(keboola.ComponentID("keboola.r-transformation-v2")))
	assert.False(t, IsScriptComponent(keboola.ComponentID("keboola.snowflake-transformation")))
}
