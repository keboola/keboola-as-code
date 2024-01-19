package dialog

import (
	"reflect"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

func TestParseJsonInput(t *testing.T) {
	t.Parallel()
	type args struct {
		fileName string
	}
	tests := []struct {
		name    string
		args    args
		want    *keboola.CreateTableRequest
		wantErr bool
	}{
		{
			name: "Parse Test", args: args{fileName: "/definition.json"}, want: &keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
							Definition: keboola.ColumnDefinition{
								Type: "INT",
							},
							BaseType: keboola.TypeNumeric,
						},
						{
							Name: "name",
							Definition: keboola.ColumnDefinition{
								Type: "TEXT",
							},
							BaseType: "STRING",
						},
					},
				},
				Name: "my-new-table",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseJSONInputForCreateTable(tt.args.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJsonInputForCreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseJsonInputForCreateTable() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetCreateRequest(t *testing.T) {
	t.Parallel()
	type args struct {
		opts table.Options
	}
	tests := []struct {
		name string
		args args
		want table.Options
	}{
		{
			name: "getCreateTableRequest",
			args: args{opts: table.Options{
				CreateTableRequest: keboola.CreateTableRequest{},
				BucketKey:          keboola.BucketKey{},
				Columns:            []string{"id", "name"},
				Name:               "test_table",
				PrimaryKey:         []string{"id"},
			}}, want: table.Options{
				CreateTableRequest: keboola.CreateTableRequest{
					TableDefinition: keboola.TableDefinition{
						PrimaryKeyNames: []string{"id"},
						Columns: []keboola.Column{
							{
								Name: "id",
								Definition: keboola.ColumnDefinition{
									Type: "STRING",
								},
								BaseType: keboola.TypeString,
							},
							{
								Name: "name",
								Definition: keboola.ColumnDefinition{
									Type: "STRING",
								},
								BaseType: keboola.TypeString,
							},
						},
					},
					Name: "test_table",
				},
				BucketKey:  keboola.BucketKey{},
				Columns:    []string{"id", "name"},
				Name:       "test_table",
				PrimaryKey: []string{"id"},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, getOptionCreateRequest(tt.args.opts), "getOptionCreateRequest(%v)", tt.args.opts)
		})
	}
}
