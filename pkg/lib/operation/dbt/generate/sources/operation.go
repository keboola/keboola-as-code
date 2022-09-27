package sources

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Fs() filesystem.Fs
	Logger() log.Logger
	Tracer() trace.Tracer
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	StorageApiClient() client.Sender
}

const sourcesPath = "models/_sources"

type SourceFile struct {
	Version int      `yaml:"version"`
	Sources []Source `yaml:"sources"`
}

type Source struct {
	Name          string          `yaml:"name"`
	Freshness     SourceFreshness `yaml:"freshness"`
	Database      string          `yaml:"database"`
	Schema        string          `yaml:"schema"`
	LoadedAtField string          `yaml:"loaded_at_field"` //nolint:tagliatelle
	Tables        []SourceTable   `yaml:"tables"`
}

type SourceTable struct {
	Name    string              `yaml:"name"`
	Quoting SourceTableQuoting  `yaml:"quoting"`
	Columns []SourceTableColumn `yaml:"columns"`
}

type SourceTableColumn struct {
	Name  string   `yaml:"name"`
	Tests []string `yaml:"tests"`
}

type SourceTableQuoting struct {
	Database   bool `yaml:"database"`
	Schema     bool `yaml:"schema"`
	Identifier bool `yaml:"identifier"`
}

type SourceFreshness struct {
	WarnAfter SourceFreshnessWarnAfter `yaml:"warn_after"` //nolint:tagliatelle
}

type SourceFreshnessWarnAfter struct {
	Count  int    `yaml:"count"`
	Period string `yaml:"period"`
}

func Run(ctx context.Context, targetName string, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.generate.sources")
	defer telemetry.EndSpan(span, &err)

	// Check that we are in dbt directory
	if _, _, err := d.LocalDbtProject(ctx); err != nil {
		return err
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
		sourcesDef := generateSourcesDefinition(targetName, bucketID, tables)
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

func generateSourcesDefinition(targetName string, bucketID storageapi.BucketID, tablesList []*storageapi.Table) SourceFile {
	sourceTables := make([]SourceTable, 0)
	for _, table := range tablesList {
		sourceTable := SourceTable{
			Name: table.Name,
			Quoting: SourceTableQuoting{
				Database:   true,
				Schema:     true,
				Identifier: true,
			},
		}
		if len(table.PrimaryKey) > 0 {
			sourceColumns := make([]SourceTableColumn, 0)
			for _, primaryKey := range table.PrimaryKey {
				sourceColumns = append(sourceColumns, SourceTableColumn{
					Name:  fmt.Sprintf(`"%s"`, primaryKey),
					Tests: []string{"unique", "not_null"},
				})
			}
			sourceTable.Columns = sourceColumns
		}
		sourceTables = append(sourceTables, sourceTable)
	}
	return SourceFile{
		Version: 2,
		Sources: []Source{
			{
				Name: string(bucketID),
				Freshness: SourceFreshness{SourceFreshnessWarnAfter{
					Count:  1,
					Period: "day",
				}},
				Database:      fmt.Sprintf("{{ env_var(\"DBT_KBC_%s_DATABASE\") }}", strings.ToUpper(targetName)),
				Schema:        string(bucketID),
				LoadedAtField: `"_timestamp"`,
				Tables:        sourceTables,
			},
		},
	}
}
