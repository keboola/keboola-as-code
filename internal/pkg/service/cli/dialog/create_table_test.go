package dialog

import (
	"reflect"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
)

func Test_parseJsonInput(t *testing.T) {
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
			name: "Parse Test", args: args{fileName: "/Users/petr/keboola/keboola-as-code/test/cli/remote-create/table/in/definition.json"}, want: &keboola.CreateTableRequest{
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
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJsonInput(tt.args.fileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJsonInput() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseJsonInput() got = %v, want %v", got, tt.want)
			}
		})
	}
}
