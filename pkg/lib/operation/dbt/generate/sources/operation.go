package sources

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Fs() filesystem.Fs
	Logger() log.Logger
	StorageApiClient() client.Sender
	Tracer() trace.Tracer
}

const sourcesPath = "models/_sources"

type sourceFile struct {
	version int      `yaml:"version"`
	sources []source `yaml:"sources"`
}

type source struct {
	name          string          `yaml:"name"`
	freshness     sourceFreshness `yaml:"freshness"`
	database      string          `yaml:"database"`
	schema        string          `yaml:"schema"`
	loadedAtField string          `yaml:"loaded_at_field"` //nolint:tagliatelle
	tables        []sourceTable   `yaml:"tables"`
}

type sourceTable struct {
	name    string              `yaml:"name"`
	quoting sourceTableQuoting  `yaml:"quoting"`
	columns []sourceTableColumn `yaml:"columns"`
}

type sourceTableColumn struct {
	name  string   `yaml:"name"`
	tests []string `yaml:"tests"`
}

type sourceTableQuoting struct {
	database   bool `yaml:"database"`
	schema     bool `yaml:"schema"`
	identifier bool `yaml:"identifier"`
}

type sourceFreshness struct {
	warnAfter sourceFreshnessWarnAfter `yaml:"warn_after"` //nolint:tagliatelle
}

type sourceFreshnessWarnAfter struct {
	count  int    `yaml:"count"`
	period string `yaml:"period"`
}

func Run(ctx context.Context, opts dialog.TargetNameOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.generate.sources")
	defer telemetry.EndSpan(span, &err)

	// Check that we are in dbt directory
	if !d.Fs().Exists(`dbt_project.yml`) {
		return fmt.Errorf(`missing file "dbt_project.yml" in the current directory`)
	}

	if !d.Fs().Exists(sourcesPath) {
		err = d.Fs().Mkdir(sourcesPath)
		if err != nil {
			return err
		}
	}

	tablesList, err := storageapi.ListTablesRequest(storageapi.WithBuckets()).Send(ctx, d.StorageApiClient())
	if err != nil {
		return err
	}
	tablesByBuckets := tablesByBucketsMap(*tablesList)

	for bucketID, tables := range tablesByBuckets {
		sourcesDef := generateSourcesDefinition(opts.Name, bucketID, tables)
		yamlEnc, err := yaml.Marshal(&sourcesDef)
		if err != nil {
			return err
		}
		err = d.Fs().WriteFile(filesystem.NewRawFile(fmt.Sprintf("%s/%s.yml", sourcesPath, bucketID), string(yamlEnc)))
		if err != nil {
			return err
		}
	}

	d.Logger().Infof(`Sources stored in "%s" directory.`, sourcesPath)
	return nil
}

func tablesByBucketsMap(tablesList []*storageapi.Table) map[storageapi.BucketID][]*storageapi.Table {
	tablesByBuckets := make(map[storageapi.BucketID][]*storageapi.Table)
	for _, table := range tablesList {
		bucket, ok := tablesByBuckets[table.Bucket.ID]
		if !ok {
			bucket = make([]*storageapi.Table, 0)
		}
		bucket = append(bucket, table)
		tablesByBuckets[table.Bucket.ID] = bucket
	}
	return tablesByBuckets
}

func generateSourcesDefinition(targetName string, bucketID storageapi.BucketID, tablesList []*storageapi.Table) sourceFile {
	sourceTables := make([]sourceTable, 0)
	for _, table := range tablesList {
		sourceTable := sourceTable{
			name: table.Name,
			quoting: sourceTableQuoting{
				database:   true,
				schema:     true,
				identifier: true,
			},
		}
		if len(table.PrimaryKey) > 0 {
			sourceColumns := make([]sourceTableColumn, 0)
			for _, primaryKey := range table.PrimaryKey {
				sourceColumns = append(sourceColumns, sourceTableColumn{
					name:  fmt.Sprintf(`"%s"`, primaryKey),
					tests: []string{"unique", "not_null"},
				})
			}
			sourceTable.columns = sourceColumns
		}
		sourceTables = append(sourceTables, sourceTable)
	}
	return sourceFile{
		version: 2,
		sources: []source{
			{
				name: string(bucketID),
				freshness: sourceFreshness{sourceFreshnessWarnAfter{
					count:  1,
					period: "day",
				}},
				database:      fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_DATABASE\") }}", strings.ToUpper(targetName)),
				schema:        string(bucketID),
				loadedAtField: `"_timestamp"`,
				tables:        sourceTables,
			},
		},
	}
}
