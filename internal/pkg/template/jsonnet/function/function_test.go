package function

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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

	type args struct {
		components *model.ComponentsMap
	}
	tests := []struct {
		name string
		args args
		want keboola.ComponentID
		err  error
	}{
		{name: "error", args: args{emptyComponent}, err: errors.New("no Snowflake Writer component found")},
		{name: "error-empty-component", args: args{emptyComponent}, err: errors.New("no Snowflake Writer component found")},
		{name: "gcp-s3", args: args{components}, want: SnowflakeWriterIDGCPS3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := SnowflakeWriterComponentID(tt.args.components)

			writer, err := got.Func([]any{})
			if tt.err != nil {
				assert.Error(t, tt.err, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, keboola.ComponentID(writer.(string)))
		})
	}
}
