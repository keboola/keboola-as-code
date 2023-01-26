package sources

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	KeboolaAPIClient() *keboola.API
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, targetName string, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.generate.sources")
	defer telemetry.EndSpan(span, &err)

	// Get dbt project
	project, _, err := d.LocalDbtProject(ctx)
	if err != nil {
		return err
	}
	fs := project.Fs()

	if !fs.Exists(dbt.SourcesPath) {
		err = fs.Mkdir(dbt.SourcesPath)
		if err != nil {
			return err
		}
	}

	tablesList, err := d.KeboolaAPIClient().ListTablesRequest(keboola.WithBuckets()).Send(ctx)
	if err != nil {
		return err
	}
	tablesByBuckets := tablesByBucketsMap(*tablesList)

	for bucketID, tables := range tablesByBuckets {
		sourcesDef := generateSourcesDefinition(targetName, bucketID, tables)
		yamlEnc, err := yaml.Marshal(&sourcesDef)
		if err != nil {
			return err
		}
		err = fs.WriteFile(filesystem.NewRawFile(fmt.Sprintf("%s/%s.yml", dbt.SourcesPath, bucketID), string(yamlEnc)))
		if err != nil {
			return err
		}
	}

	d.Logger().Infof(`Sources stored in "%s" directory.`, dbt.SourcesPath)
	return nil
}

func tablesByBucketsMap(tablesList []*keboola.Table) map[keboola.BucketID][]*keboola.Table {
	tablesByBuckets := make(map[keboola.BucketID][]*keboola.Table)
	for _, table := range tablesList {
		bucket, ok := tablesByBuckets[table.Bucket.ID]
		if !ok {
			bucket = make([]*keboola.Table, 0)
		}
		bucket = append(bucket, table)
		tablesByBuckets[table.Bucket.ID] = bucket
	}
	return tablesByBuckets
}

func generateSourcesDefinition(targetName string, bucketID keboola.BucketID, tablesList []*keboola.Table) dbt.SourceFile {
	sourceTables := make([]dbt.SourceTable, 0)
	for _, table := range tablesList {
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
				Name: bucketID.String(),
				Freshness: dbt.SourceFreshness{
					WarnAfter: dbt.SourceFreshnessWarnAfter{
						Count:  1,
						Period: "day",
					},
				},
				Database:      fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_DATABASE\") }}", strings.ToUpper(targetName)),
				Schema:        bucketID.String(),
				LoadedAtField: `"_timestamp"`,
				Tables:        sourceTables,
			},
		},
	}
}
