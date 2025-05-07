package listbuckets

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestGroupTables(t *testing.T) {
	t.Parallel()

	mainBucket := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: 123,
			BucketID: keboola.MustParseBucketID("out.c-main"),
		},
		DisplayName: "main",
	}
	secondBucket := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: 123,
			BucketID: keboola.MustParseBucketID("out.c-second"),
		},
		DisplayName: "second",
	}
	linkedBucket := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: 123,
			BucketID: keboola.MustParseBucketID("out.c-linked"),
		},
		DisplayName: "linked",
	}
	mainTable1 := keboola.Table{
		TableKey: keboola.TableKey{
			BranchID: 123,
			TableID:  keboola.MustParseTableID("out.c-main.products"),
		},
		Name:        "products",
		DisplayName: "Products",
		Bucket:      mainBucket,
	}
	mainTable2 := keboola.Table{
		TableKey: keboola.TableKey{
			BranchID: 123,
			TableID:  keboola.MustParseTableID("out.c-main.categories"),
		},
		Name:        "categories",
		DisplayName: "Categories",
		Bucket:      mainBucket,
	}
	secTable1 := keboola.Table{
		TableKey: keboola.TableKey{
			BranchID: 123,
			TableID:  keboola.MustParseTableID("out.c-second.products"),
		},
		Name:        "products",
		DisplayName: "Products",
		Bucket:      secondBucket,
	}
	secTable2 := keboola.Table{
		TableKey: keboola.TableKey{
			BranchID: 123,
			TableID:  keboola.MustParseTableID("out.c-second.third"),
		},
		Name:        "third",
		DisplayName: "Third",
		Bucket:      secondBucket,
	}
	linkedTable1 := keboola.Table{
		TableKey: keboola.TableKey{
			BranchID: 123,
			TableID:  keboola.MustParseTableID("out.c-linked.foo"),
		},
		SourceTable: &keboola.SourceTable{
			Project: keboola.SourceProject{
				ID:   12345,
				Name: "Project12345",
			},
			ID:   keboola.MustParseTableID("out.c-shared.table1"),
			Name: "Table1",
		},
		Name:        "foo",
		DisplayName: "Foo",
		Bucket:      linkedBucket,
	}
	linkedTable2 := keboola.Table{
		TableKey: keboola.TableKey{
			BranchID: 123,
			TableID:  keboola.MustParseTableID("out.c-linked.bar"),
		},
		SourceTable: &keboola.SourceTable{
			Project: keboola.SourceProject{
				ID:   12345,
				Name: "Project12345",
			},
			ID:   keboola.MustParseTableID("out.c-shared.table2"),
			Name: "Table2",
		},
		Name:        "bar",
		DisplayName: "Bar",
		Bucket:      linkedBucket,
	}

	in := []*keboola.Table{&mainTable1, &secTable1, &mainTable2, &secTable2, &linkedTable1, &linkedTable2}
	res := groupTables("target1", in)

	assert.Equal(t, []Bucket{
		{
			SourceName:  mainBucket.BucketID.String(),
			Schema:      mainBucket.BucketID.String(),
			DatabaseEnv: "DBT_KBC_TARGET1_DATABASE",
			Tables:      []keboola.Table{mainTable1, mainTable2},
		},
		{
			SourceName:  secondBucket.BucketID.String(),
			Schema:      secondBucket.BucketID.String(),
			DatabaseEnv: "DBT_KBC_TARGET1_DATABASE",
			Tables:      []keboola.Table{secTable1, secTable2},
		},
		{
			LinkedProjectID: 12345,
			SourceName:      linkedBucket.BucketID.String(), // defined by the linked bucket name
			Schema:          "out.c-shared",                 // defined by the source bucket name
			DatabaseEnv:     "DBT_KBC_TARGET1_12345_DATABASE",
			Tables:          []keboola.Table{linkedTable1, linkedTable2},
		},
	}, res)
}
