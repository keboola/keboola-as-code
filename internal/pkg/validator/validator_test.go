package validator

import (
	"context"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStruct1 struct {
	Field1      string        `json:"field1" validate:"required"`
	Field2      string        `yaml:"field2" validate:"required"`
	Field3      string        `json:"-" validate:"required"`
	Field4      string        `validate:"required"`
	Nested      []testStruct2 `validate:"dive"`
	testStruct2               // anonymous
}

type testStruct2 struct {
	Field4 string `json:"field4" validate:"required"`
}

func TestValidateStruct(t *testing.T) {
	t.Parallel()
	err := New().Validate(context.Background(), testStruct1{Nested: []testStruct2{{}, {}}})
	expected := `
- "field1" is a required field
- "field2" is a required field
- "Field3" is a required field
- "Field4" is a required field
- "Nested[0].field4" is a required field
- "Nested[1].field4" is a required field
- "field4" is a required field
`
	require.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateStructWithNamespace(t *testing.T) {
	t.Parallel()
	err := New().ValidateCtx(context.Background(), testStruct1{Nested: []testStruct2{{}, {}}}, "dive", "my.value")
	expected := `
- "my.value.field1" is a required field
- "my.value.field2" is a required field
- "my.value.Field3" is a required field
- "my.value.Field4" is a required field
- "my.value.Nested[0].field4" is a required field
- "my.value.Nested[1].field4" is a required field
- "my.value.field4" is a required field
`
	require.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateSlice(t *testing.T) {
	t.Parallel()
	err := New().Validate(context.Background(), []testStruct2{{}, {}})
	expected := `
- "[0].field4" is a required field
- "[1].field4" is a required field
`
	require.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateSliceWithNamespace(t *testing.T) {
	t.Parallel()
	err := New().ValidateCtx(context.Background(), []testStruct2{{}, {}}, "dive", "my.value")
	expected := `
- "my.value.[0].field4" is a required field
- "my.value.[1].field4" is a required field
`
	require.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateValue(t *testing.T) {
	t.Parallel()
	err := New().ValidateValue("", "required")
	require.Error(t, err)
	assert.Equal(t, `is a required field`, err.Error())
}

func TestValidateValueAddNamespace(t *testing.T) {
	t.Parallel()
	err := New().ValidateCtx(context.Background(), "", "required", "my.value")
	require.Error(t, err)
	assert.Equal(t, `"my.value" is a required field`, err.Error())
}

func TestValidateErrorMsgFunc(t *testing.T) {
	t.Parallel()
	rule := Rule{
		Tag: "my_rule",
		Func: func(fl validator.FieldLevel) bool {
			return false
		},
		ErrorMsgFunc: func(fe validator.FieldError) string {
			if fe.Value() == "foo" {
				return "error message for foo"
			}
			return "other error message"
		},
	}

	err := New(rule).ValidateCtx(context.Background(), "foo", "my_rule", "my.value")
	require.Error(t, err)
	assert.Equal(t, `"my.value" error message for foo`, err.Error())

	err = New(rule).ValidateCtx(context.Background(), "other", "my_rule", "my.value")
	require.Error(t, err)
	assert.Equal(t, `"my.value" other error message`, err.Error())
}

func TestValidatorRequiredInProject(t *testing.T) {
	t.Parallel()
	v := New()

	// Project
	projectCtx := context.Background()
	err := v.ValidateCtx(projectCtx, `value`, `required_in_project`, `some_field`)
	require.NoError(t, err)
	err = v.ValidateCtx(projectCtx, ``, `required_in_project`, `some_field`)
	require.Error(t, err)
	assert.Equal(t, `"some_field" is a required field`, err.Error())

	// Template
	templateCtx := context.WithValue(context.Background(), DisableRequiredInProjectKey, true)
	err = v.ValidateCtx(templateCtx, ``, `required_in_project`, `some_field`)
	require.NoError(t, err)
	err = v.ValidateCtx(templateCtx, `value`, `required_in_project`, `some_field`)
	require.NoError(t, err)
}

func TestValidatorRequiredNotEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	v := New()

	// String
	err := v.ValidateCtx(ctx, `value`, `required_not_empty`, `some_field`)
	require.NoError(t, err)
	err = v.ValidateCtx(ctx, ``, `required_not_empty`, `some_field`)
	require.Error(t, err)
	assert.Equal(t, `"some_field" is a required field`, err.Error())

	// Array
	err = v.ValidateCtx(ctx, []int{1, 2, 3}, `required_not_empty`, `some_field`)
	require.NoError(t, err)
	err = v.ValidateCtx(ctx, []int{}, `required_not_empty`, `some_field`)
	require.Error(t, err)
	assert.Equal(t, `"some_field" is a required field`, err.Error())
	err = v.ValidateCtx(ctx, nil, `required_not_empty`, `some_field`)
	require.Error(t, err)
	assert.Equal(t, `"some_field" is a required field`, err.Error())
}

func TestValidatorAlphaNumDash(t *testing.T) {
	t.Parallel()
	cases := []struct{ value, error string }{
		{"123", ""},
		{"abc", ""},
		{"123abc", ""},
		{"123-abc-456-def", ""},
		{"#-123-abc", "some_field can only contain alphanumeric characters and dash"},
		{"#-123-abc", "some_field can only contain alphanumeric characters and dash"},
		{"123-abc-#", "some_field can only contain alphanumeric characters and dash"},
	}

	v := New()
	for i, c := range cases {
		err := v.ValidateCtx(context.Background(), c.value, `alphanumdash`, `some_field`)
		if c.error == "" {
			require.NoError(t, err, `case: %d`, i+1)
		} else {
			require.Error(t, err, c.error, `case: %d`, i+1)
		}
	}
}

func TestValidatorTemplateIcon(t *testing.T) {
	t.Parallel()
	cases := []struct{ value, error string }{
		{"component:foo-bar", ""},
		{"common:upload", ""},
		{"common:download", ""},
		{"common:settings", ""},
		{"common:import", ""},
		{"common:foo", "some_field does not contain an allowed icon"},
		{"common:", "some_field does not contain an allowed icon"},
		{"component:", "some_field does not contain an allowed icon"},
		{"", "some_field does not contain an allowed icon"},
	}

	v := New()
	for i, c := range cases {
		err := v.ValidateCtx(context.Background(), c.value, `templateicon`, `some_field`)
		if c.error == "" {
			require.NoError(t, err, `case: %d`, i+1)
		} else {
			require.Error(t, err, c.error, `case: %d`, i+1)
		}
	}
}

func TestValidatorMarkdownLength(t *testing.T) {
	t.Parallel()

	cases := []struct{ value, error string }{
		{"test", ""},
		{"### test test", ""},
		{"[test](https://google.com/)", ""},
		{"this is more than 10 characters", "some_field exceeded maximum length of 10"},
		{"[this is also more than 10 characters](https://google.com/)", "some_field exceeded maximum length of 10"},
	}

	v := New()
	for i, c := range cases {
		err := v.ValidateCtx(context.Background(), c.value, `mdmax=10`, `some_field`)
		if c.error == "" {
			require.NoError(t, err, `case: %d`, i+1)
		} else {
			require.Error(t, err, c.error, `case: %d`, i+1)
		}
	}
}
