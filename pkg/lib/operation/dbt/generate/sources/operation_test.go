package sources

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
)

func TestTablesByBucketsMap(t *testing.T) {
	t.Parallel()

	mainBucket := &storageapi.Bucket{
		ID:             "out.c-main",
		Uri:            "/uri",
		Name:           "main",
		DisplayName:    "main",
		Stage:          "out",
		Description:    "",
		Created:        storageapi.Time{},
		LastChangeDate: &storageapi.Time{},
		IsReadOnly:     false,
		DataSizeBytes:  0,
		RowsCount:      0,
	}
	secondBucket := &storageapi.Bucket{
		ID:             "out.c-second",
		Uri:            "/uri",
		Name:           "second",
		DisplayName:    "second",
		Stage:          "out",
		Description:    "",
		Created:        storageapi.Time{},
		LastChangeDate: &storageapi.Time{},
		IsReadOnly:     false,
		DataSizeBytes:  0,
		RowsCount:      0,
	}
	mainTable1 := &storageapi.Table{
		ID:             "out.c-main.products",
		Uri:            "/uri",
		Name:           "products",
		DisplayName:    "Products",
		PrimaryKey:     nil,
		Created:        storageapi.Time{},
		LastImportDate: storageapi.Time{},
		LastChangeDate: storageapi.Time{},
		RowsCount:      0,
		DataSizeBytes:  0,
		Columns:        nil,
		Metadata:       nil,
		ColumnMetadata: nil,
		Bucket:         mainBucket,
	}
	mainTable2 := &storageapi.Table{
		ID:             "out.c-main.categories",
		Uri:            "/uri",
		Name:           "categories",
		DisplayName:    "Categories",
		PrimaryKey:     nil,
		Created:        storageapi.Time{},
		LastImportDate: storageapi.Time{},
		LastChangeDate: storageapi.Time{},
		RowsCount:      0,
		DataSizeBytes:  0,
		Columns:        nil,
		Metadata:       nil,
		ColumnMetadata: nil,
		Bucket:         mainBucket,
	}
	secTable1 := &storageapi.Table{
		ID:             "out.c-second.products",
		Uri:            "/uri",
		Name:           "products",
		DisplayName:    "Products",
		PrimaryKey:     nil,
		Created:        storageapi.Time{},
		LastImportDate: storageapi.Time{},
		LastChangeDate: storageapi.Time{},
		RowsCount:      0,
		DataSizeBytes:  0,
		Columns:        nil,
		Metadata:       nil,
		ColumnMetadata: nil,
		Bucket:         secondBucket,
	}
	secTable2 := &storageapi.Table{
		ID:             "out.c-second.third",
		Uri:            "/uri",
		Name:           "third",
		DisplayName:    "Third",
		PrimaryKey:     nil,
		Created:        storageapi.Time{},
		LastImportDate: storageapi.Time{},
		LastChangeDate: storageapi.Time{},
		RowsCount:      0,
		DataSizeBytes:  0,
		Columns:        nil,
		Metadata:       nil,
		ColumnMetadata: nil,
		Bucket:         secondBucket,
	}
	in := []*storageapi.Table{mainTable1, secTable1, mainTable2, secTable2}

	res := tablesByBucketsMap(in)
	assert.Equal(t, map[storageapi.BucketID][]*storageapi.Table{
		"out.c-main":   {mainTable1, mainTable2},
		"out.c-second": {secTable1, secTable2},
	}, res)
}

func TestGenerateSourcesDefinition(t *testing.T) {
	t.Parallel()

	mainBucket := &storageapi.Bucket{
		ID:             "out.c-main",
		Uri:            "/uri",
		Name:           "main",
		DisplayName:    "main",
		Stage:          "out",
		Description:    "",
		Created:        storageapi.Time{},
		LastChangeDate: &storageapi.Time{},
		IsReadOnly:     false,
		DataSizeBytes:  0,
		RowsCount:      0,
	}
	mainTable1 := &storageapi.Table{
		ID:             "out.c-main.products",
		Uri:            "/uri",
		Name:           "products",
		DisplayName:    "Products",
		PrimaryKey:     []string{"primary1", "primary2"},
		Created:        storageapi.Time{},
		LastImportDate: storageapi.Time{},
		LastChangeDate: storageapi.Time{},
		RowsCount:      0,
		DataSizeBytes:  0,
		Columns:        nil,
		Metadata:       nil,
		ColumnMetadata: nil,
		Bucket:         mainBucket,
	}
	mainTable2 := &storageapi.Table{
		ID:             "out.c-main.categories",
		Uri:            "/uri",
		Name:           "categories",
		DisplayName:    "Categories",
		PrimaryKey:     nil,
		Created:        storageapi.Time{},
		LastImportDate: storageapi.Time{},
		LastChangeDate: storageapi.Time{},
		RowsCount:      0,
		DataSizeBytes:  0,
		Columns:        nil,
		Metadata:       nil,
		ColumnMetadata: nil,
		Bucket:         mainBucket,
	}

	res := generateSourcesDefinition("target1", "out.c-main", []*storageapi.Table{mainTable1, mainTable2})
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
	assert.NoError(t, err)
	assert.Equal(t, `version: 2
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
`, string(yamlEnc))
}
