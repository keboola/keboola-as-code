package function

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestSnowflakeWriterComponentID(t *testing.T) {
	t.Parallel()
	emptyComponent := model.NewComponentsMap(nil)
	components := model.NewComponentsMap(keboola.Components{
		{
			ComponentKey: keboola.ComponentKey{ID: SnowflakeWriterIDGCP},
			Type:         "other",
			Name:         "Bar",
			Data:         keboola.ComponentData{},
		},
		{
			ComponentKey: keboola.ComponentKey{ID: SnowflakeWriterIDGCPS3},
			Type:         "other",
			Name:         "Foo",
			Data:         keboola.ComponentData{},
		},
	})
	componentAWS := model.NewComponentsMap(keboola.Components{
		{
			ComponentKey: keboola.ComponentKey{ID: SnowflakeWriterIDAws},
			Type:         "other",
			Name:         "Bar",
			Data:         keboola.ComponentData{},
		},
	})
	componentAZURE := model.NewComponentsMap(keboola.Components{
		{
			ComponentKey: keboola.ComponentKey{ID: SnowflakeWriterIDAzure},
			Type:         "other",
			Name:         "Bar",
			Data:         keboola.ComponentData{},
		},
	})

	type args struct {
		components *model.ComponentsMap
		backends   []string
	}
	tests := []struct {
		name string
		args args
		want keboola.ComponentID
		err  error
	}{
		{name: "error", args: args{emptyComponent, nil}, err: errors.New("no Snowflake Writer component found")},
		{name: "error-empty-component", args: args{emptyComponent, nil}, err: errors.New("no Snowflake Writer component found")},
		{name: "aws", args: args{componentAWS, []string{project.BackendSnowflake}}, want: SnowflakeWriterIDAws},
		{name: "azure", args: args{componentAZURE, []string{project.BackendSnowflake}}, want: SnowflakeWriterIDAzure},
		{name: "gcp-s3", args: args{components, []string{project.BackendSnowflake}}, want: SnowflakeWriterIDGCPS3},
		{name: "gcp", args: args{components, []string{project.BackendBigQuery}}, want: SnowflakeWriterIDGCP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SnowflakeWriterComponentID(tt.args.components, tt.args.backends)

			writer, err := got.Func([]any{})
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, keboola.ComponentID(writer.(string)))
		})
	}
}
