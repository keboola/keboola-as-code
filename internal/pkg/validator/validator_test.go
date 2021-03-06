package validator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
)

type testStruct1 struct {
	Field1      string        `json:"field1" validate:"required"`
	Field2      string        `json:"-" validate:"required"`
	Field3      string        `validate:"required"`
	Nested      []testStruct2 `validate:"dive"`
	testStruct2               // anonymous
}

type testStruct2 struct {
	Field4 string `json:"field4" validate:"required"`
}

func TestValidateStruct(t *testing.T) {
	t.Parallel()
	err := Validate(context.Background(), testStruct1{Nested: []testStruct2{{}, {}}})
	expected := `
- field1 is a required field
- Field2 is a required field
- Field3 is a required field
- Nested[0].field4 is a required field
- Nested[1].field4 is a required field
- field4 is a required field
`
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateStructWithNamespace(t *testing.T) {
	t.Parallel()
	err := ValidateCtx(context.Background(), testStruct1{Nested: []testStruct2{{}, {}}}, "dive", "my.value")
	expected := `
- my.value.field1 is a required field
- my.value.Field2 is a required field
- my.value.Field3 is a required field
- my.value.Nested[0].field4 is a required field
- my.value.Nested[1].field4 is a required field
- my.value.field4 is a required field
`
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateSlice(t *testing.T) {
	t.Parallel()
	err := Validate(context.Background(), []testStruct2{{}, {}})
	expected := `
- [0].field4 is a required field
- [1].field4 is a required field
`
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateSliceWithNamespace(t *testing.T) {
	t.Parallel()
	err := ValidateCtx(context.Background(), []testStruct2{{}, {}}, "dive", "my.value")
	expected := `
- my.value.[0].field4 is a required field
- my.value.[1].field4 is a required field
`
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateValue(t *testing.T) {
	t.Parallel()
	err := ValidateCtx(context.Background(), "", "required", "")
	assert.Error(t, err)
	assert.Equal(t, `is a required field`, err.Error())
}

func TestValidateValueAddNamespace(t *testing.T) {
	t.Parallel()
	err := ValidateCtx(context.Background(), "", "required", "my.value")
	assert.Error(t, err)
	assert.Equal(t, `my.value is a required field`, err.Error())
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

	err := ValidateCtx(context.Background(), "foo", "my_rule", "my.value", rule)
	assert.Error(t, err)
	assert.Equal(t, `my.value error message for foo`, err.Error())

	err = ValidateCtx(context.Background(), "other", "my_rule", "my.value", rule)
	assert.Error(t, err)
	assert.Equal(t, `my.value other error message`, err.Error())
}

func TestValidatorRequiredInProject(t *testing.T) {
	t.Parallel()

	// Project
	projectCtx := context.Background()
	err := ValidateCtx(projectCtx, `value`, `required_in_project`, `some_field`)
	assert.NoError(t, err)
	err = ValidateCtx(projectCtx, ``, `required_in_project`, `some_field`)
	assert.Error(t, err)
	assert.Equal(t, "some_field is a required field", err.Error())

	// Template
	templateCtx := context.WithValue(context.Background(), DisableRequiredInProjectKey, true)
	err = ValidateCtx(templateCtx, ``, `required_in_project`, `some_field`)
	assert.NoError(t, err)
	err = ValidateCtx(templateCtx, `value`, `required_in_project`, `some_field`)
	assert.NoError(t, err)
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

	for i, c := range cases {
		err := ValidateCtx(context.Background(), c.value, `alphanumdash`, `some_field`)
		if c.error == "" {
			assert.NoError(t, err, fmt.Sprintf("case: %d", i+1))
		} else {
			assert.Error(t, err, c.error, fmt.Sprintf("case: %d", i+1))
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

	for i, c := range cases {
		err := ValidateCtx(context.Background(), c.value, `templateicon`, `some_field`)
		if c.error == "" {
			assert.NoError(t, err, fmt.Sprintf("case: %d", i+1))
		} else {
			assert.Error(t, err, c.error, fmt.Sprintf("case: %d", i+1))
		}
	}
}
