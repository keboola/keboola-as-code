package sources

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/listbuckets"
)

func generateSourcesDefinition(bucket listbuckets.Bucket) dbt.SourceFile {
	sourceTables := make([]dbt.SourceTable, 0)
	for _, table := range bucket.Tables {
		sourceTable := dbt.SourceTable{
			Name: table.Name,
			Quoting: dbt.SourceTableQuoting{
				Database:   true,
				Schema:     true,
				Identifier: true,
			},
		}
		if len(table.PrimaryKey) > 0 {
			sourceColumns := make([]dbt.SourceTableColumn, 0)
			for _, primaryKey := range table.PrimaryKey {
				sourceColumns = append(sourceColumns, dbt.SourceTableColumn{
					Name:  fmt.Sprintf(`"%s"`, primaryKey),
					Tests: []string{"unique", "not_null"},
				})
			}
			sourceTable.Columns = sourceColumns
		}
		sourceTables = append(sourceTables, sourceTable)
	}

	return dbt.SourceFile{
		Version: 2,
		Sources: []dbt.Source{
			{
				Name: bucket.SourceName,
				Freshness: dbt.SourceFreshness{
					WarnAfter: dbt.SourceFreshnessWarnAfter{
						Count:  1,
						Period: "day",
					},
				},
				Database:      fmt.Sprintf(`{{ env_var("%s") }}`, bucket.DatabaseEnv),
				Schema:        bucket.Schema,
				LoadedAtField: `"_timestamp"`,
				Tables:        sourceTables,
			},
		},
	}
}
