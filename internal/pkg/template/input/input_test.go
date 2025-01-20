package input

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
)

func TestInput_ValidateUserInput(t *testing.T) {
	t.Parallel()

	input := Input{
		ID:          "input.id",
		Name:        "my input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
		Rules:       "gte=5,lte=10",
	}
	err := input.ValidateUserInput(context.Background(), 1)
	require.Error(t, err)
	assert.Equal(t, "my input must be 5 or greater", err.Error())

	err = input.ValidateUserInput(context.Background(), "1")
	require.Error(t, err)
	assert.Equal(t, "my input should be int, got string", err.Error())

	require.Error(t, err)
	require.NoError(t, input.ValidateUserInput(context.Background(), 7))

	input = Input{
		ID:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "bool",
		Kind:        "confirm",
	}
	err = input.ValidateUserInput(context.Background(), 1)
	require.Error(t, err)
	assert.Equal(t, "input should be bool, got int", err.Error())
	require.NoError(t, input.ValidateUserInput(context.Background(), true))
}

func TestInput_ValidateUserInputOAuth(t *testing.T) {
	t.Parallel()

	input := Input{
		ID:          "input.oauth",
		Name:        "oauth",
		Description: "oauth",
		Type:        "object",
		Kind:        "oauth",
		ComponentID: "foo.bar",
	}
	err := input.ValidateUserInput(context.Background(), []string{"one", "two"})
	require.Error(t, err)
	assert.Equal(t, "oauth should be object, got slice", err.Error())

	err = input.ValidateUserInput(context.Background(), map[string]any{"a": "b"})
	require.NoError(t, err)
}

func TestInput_Available(t *testing.T) {
	t.Parallel()

	// Check If evaluated as true
	input := Input{
		ID:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
		If:          "facebook_integration == true",
	}
	params := make(map[string]any, 1)
	params["facebook_integration"] = true
	v, err := input.Available(params)
	assert.True(t, v)
	require.NoError(t, err)

	// Check empty If evaluated as true
	input = Input{
		ID:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
	}
	v, err = input.Available(nil)
	assert.True(t, v)
	require.NoError(t, err)

	// Check If evaluated as false
	input = Input{
		ID:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
		If:          "facebook_integration == true",
	}
	params = make(map[string]any, 1)
	params["facebook_integration"] = false
	v, err = input.Available(params)
	assert.False(t, v)
	require.NoError(t, err)
}

func TestInput_MatchesAvailableBackend(t *testing.T) {
	t.Parallel()
	type fields struct {
		ID          string
		Name        string
		Description string
		Backend     *string
		Type        Type
		Kind        Kind
	}
	type args struct {
		backends []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "matches backend",
			fields: fields{
				ID:          "input.id",
				Name:        "input",
				Description: "input description",
				Backend:     ptr.Ptr(project.BackendSnowflake),
				Type:        "string",
				Kind:        "input",
			},
			args: args{
				backends: []string{project.BackendSnowflake},
			},
			want: true,
		},
		{
			name: "empty backend",
			fields: fields{
				ID:          "input.id",
				Name:        "input",
				Description: "input description",
				Type:        "string",
				Kind:        "input",
			},
			args: args{
				backends: []string{project.BackendSnowflake},
			},
			want: true,
		},
		{
			name: "does not match backend",
			fields: fields{
				ID:          "input.id",
				Name:        "input",
				Description: "input description",
				Backend:     ptr.Ptr(project.BackendBigQuery),
				Type:        "string",
				Kind:        "input",
			},
			args: args{
				backends: []string{project.BackendSnowflake},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			i := Input{
				ID:          tt.fields.ID,
				Name:        tt.fields.Name,
				Description: tt.fields.Description,
				Backend:     tt.fields.Backend,
				Type:        tt.fields.Type,
				Kind:        tt.fields.Kind,
			}
			assert.Equalf(t, tt.want, i.MatchesAvailableBackend(tt.args.backends), "MatchesAvailableBackend(%v)", tt.args.backends)
		})
	}
}
