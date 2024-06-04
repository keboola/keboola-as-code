package sources

import (
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/listbuckets"
)

func TestGenerateSourcesDefinition(t *testing.T) {
	t.Parallel()

	bucket := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: 123,
			BucketID: keboola.MustParseBucketID("out.c-main"),
		},
		URI:         "/uri",
		DisplayName: "main",
	}

	dbtBucket := listbuckets.Bucket{
		SourceName:  bucket.BucketID.String(),
		Schema:      bucket.BucketID.String(),
		DatabaseEnv: "DBT_KBC_TARGET1_DATABASE",
		Tables: []keboola.Table{
			{
				TableKey: keboola.TableKey{
					BranchID: 123,
					TableID:  keboola.MustParseTableID("out.c-main.products"),
				},
				URI:         "/uri",
				Name:        "products",
				DisplayName: "Products",
				PrimaryKey:  []string{"primary1", "primary2"},
				Bucket:      bucket,
			},
			{
				TableKey: keboola.TableKey{
					BranchID: 123,
					TableID:  keboola.MustParseTableID("out.c-main.categories"),
				},
				URI:         "/uri",
				Name:        "categories",
				DisplayName: "Categories",
				Bucket:      bucket,
			},
		},
	}

	res := generateSourcesDefinition(dbtBucket)
	assert.Equal(t, dbt.SourceFile{
		Version: 2,
		Sources: []dbt.Source{
			{
				Name: "out.c-main",
				Freshness: dbt.SourceFreshness{
					WarnAfter: dbt.SourceFreshnessWarnAfter{
						Count:  1,
						Period: "day",
					},
				},
				Database:      "{{ env_var(\"DBT_KBC_TARGET1_DATABASE\") }}",
				Schema:        "out.c-main",
				LoadedAtField: `"_timestamp"`,
				Tables: []dbt.SourceTable{
					{
						Name: "products",
						Quoting: dbt.SourceTableQuoting{
							Database:   true,
							Schema:     true,
							Identifier: true,
						},
						Columns: []dbt.SourceTableColumn{
							{
								Name:  `"primary1"`,
								Tests: []string{"unique", "not_null"},
							},
							{
								Name:  `"primary2"`,
								Tests: []string{"unique", "not_null"},
							},
						},
					},
					{
						Name: "categories",
						Quoting: dbt.SourceTableQuoting{
							Database:   true,
							Schema:     true,
							Identifier: true,
						},
					},
				},
			},
		},
	}, res)

	yamlEnc, err := yaml.Marshal(&res)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(`
version: 2
sources:
- name: out.c-main
  freshness:
    warn_after:
      count: 1
      period: day
  database: '{{ env_var("DBT_KBC_TARGET1_DATABASE") }}'
  schema: out.c-main
  loaded_at_field: '"_timestamp"'
  tables:
  - name: products
    quoting:
      database: true
      schema: true
      identifier: true
    columns:
    - name: '"primary1"'
      tests:
      - unique
      - not_null
    - name: '"primary2"'
      tests:
      - unique
      - not_null
  - name: categories
    quoting:
      database: true
      schema: true
      identifier: true
    columns: []
`), strings.TrimSpace(string(yamlEnc)))
}

func TestGenerateSourcesDefinition_LinkedBucket(t *testing.T) {
	t.Parallel()

	bucket := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: 123,
			BucketID: keboola.MustParseBucketID("out.c-main"),
		},
		URI:         "/uri",
		DisplayName: "main",
	}

	dbtBucket := listbuckets.Bucket{
		LinkedProjectID: 12345,
		SourceName:      bucket.BucketID.String(),
		Schema:          bucket.BucketID.String(),
		DatabaseEnv:     "DBT_KBC_TARGET1_12345_DATABASE",
		Tables: []keboola.Table{
			{
				TableKey: keboola.TableKey{
					BranchID: 123,
					TableID:  keboola.MustParseTableID("out.c-main.products"),
				},
				URI:         "/uri",
				Name:        "products",
				DisplayName: "Products",
				PrimaryKey:  []string{"primary1", "primary2"},
				Bucket:      bucket,
			},
			{
				TableKey: keboola.TableKey{
					BranchID: 123,
					TableID:  keboola.MustParseTableID("out.c-main.categories"),
				},
				URI:         "/uri",
				Name:        "categories",
				DisplayName: "Categories",
				Bucket:      bucket,
			},
		},
	}

	res := generateSourcesDefinition(dbtBucket)
	assert.Equal(t, dbt.SourceFile{
		Version: 2,
		Sources: []dbt.Source{
			{
				Name: "out.c-main",
				Freshness: dbt.SourceFreshness{
					WarnAfter: dbt.SourceFreshnessWarnAfter{
						Count:  1,
						Period: "day",
					},
				},
				Database:      "{{ env_var(\"DBT_KBC_TARGET1_12345_DATABASE\") }}",
				Schema:        "out.c-main",
				LoadedAtField: `"_timestamp"`,
				Tables: []dbt.SourceTable{
					{
						Name: "products",
						Quoting: dbt.SourceTableQuoting{
							Database:   true,
							Schema:     true,
							Identifier: true,
						},
						Columns: []dbt.SourceTableColumn{
							{
								Name:  `"primary1"`,
								Tests: []string{"unique", "not_null"},
							},
							{
								Name:  `"primary2"`,
								Tests: []string{"unique", "not_null"},
							},
						},
					},
					{
						Name: "categories",
						Quoting: dbt.SourceTableQuoting{
							Database:   true,
							Schema:     true,
							Identifier: true,
						},
					},
				},
			},
		},
	}, res)

	yamlEnc, err := yaml.Marshal(&res)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(`
version: 2
sources:
- name: out.c-main
  freshness:
    warn_after:
      count: 1
      period: day
  database: '{{ env_var("DBT_KBC_TARGET1_12345_DATABASE") }}'
  schema: out.c-main
  loaded_at_field: '"_timestamp"'
  tables:
  - name: products
    quoting:
      database: true
      schema: true
      identifier: true
    columns:
    - name: '"primary1"'
      tests:
      - unique
      - not_null
    - name: '"primary2"'
      tests:
      - unique
      - not_null
  - name: categories
    quoting:
      database: true
      schema: true
      identifier: true
    columns: []
`), strings.TrimSpace(string(yamlEnc)))
}
