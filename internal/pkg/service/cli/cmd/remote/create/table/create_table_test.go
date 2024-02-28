package table

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestGetCreateRequest(t *testing.T) {
	t.Parallel()
	type args struct {
		columns []string
	}
	tests := []struct {
		name string
		args args
		want []keboola.Column
	}{
		{
			name: "getCreateTableRequest",
			args: args{columns: []string{"id", "name"}},
			want: []keboola.Column{
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
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, getOptionCreateRequest(tt.args.columns), "getOptionCreateRequest(%v)", tt.args.columns)
		})
	}
}
