package listbuckets

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

type Bucket struct {
	LinkedProjectID keboola.ProjectID
	Schema          string
	SourceName      string
	DatabaseEnv     string
	Tables          []keboola.Table
}

func groupTables(targetName string, in []*keboola.Table) (out []Bucket) {
	groupMap := make(map[string]Bucket)
	for _, table := range in {
		var projectID keboola.ProjectID
		var schema string
		var databaseEnv string
		if table.SourceTable == nil {
			schema = table.TableID.BucketID.String()
			databaseEnv = fmt.Sprintf("DBT_KBC_%s_DATABASE", strings.ToUpper(targetName))
		} else {
			projectID = table.SourceTable.Project.ID
			schema = table.SourceTable.ID.BucketID.String()
			databaseEnv = fmt.Sprintf("DBT_KBC_%s_%d_DATABASE", strings.ToUpper(targetName), projectID)
		}

		key := table.Bucket.BucketID.String()
		bucket, ok := groupMap[key]
		if !ok {
			bucket.LinkedProjectID = projectID
			bucket.Schema = schema
			bucket.SourceName = table.Bucket.BucketID.String()
			bucket.DatabaseEnv = databaseEnv
		}
		bucket.Tables = append(bucket.Tables, *table)
		groupMap[key] = bucket
	}

	for _, bucket := range groupMap {
		out = append(out, bucket)
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Schema < out[j].Schema
	})

	return out
}
