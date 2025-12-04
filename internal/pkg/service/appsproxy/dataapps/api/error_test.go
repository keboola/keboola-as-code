package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestError_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	type testCase struct {
		Description     string
		JSON            string
		ExpectedError   *Error
		ExpectedContext map[string]any
		ShouldFail      bool
	}

	cases := []testCase{
		{
			Description: "context as empty array",
			JSON:        `{"error":"some error","exceptionId":"123","status":"error","context":[]}`,
			ExpectedError: &Error{
				Message:     "some error",
				ExceptionID: "123",
				Context:     nil,
			},
			ExpectedContext: nil,
			ShouldFail:      false,
		},
		{
			Description: "context as empty object",
			JSON:        `{"error":"some error","exceptionId":"123","status":"error","context":{}}`,
			ExpectedError: &Error{
				Message:     "some error",
				ExceptionID: "123",
				Context:     map[string]any{},
			},
			ExpectedContext: map[string]any{},
			ShouldFail:      false,
		},
		{
			Description: "context as object with data",
			JSON:        `{"error":"some error","exceptionId":"123","status":"error","context":{"code":"apps.restartDisabled","foo":"bar"}}`,
			ExpectedError: &Error{
				Message:     "some error",
				ExceptionID: "123",
				Context:     map[string]any{"code": "apps.restartDisabled", "foo": "bar"},
			},
			ExpectedContext: map[string]any{"code": "apps.restartDisabled", "foo": "bar"},
			ShouldFail:      false,
		},
		{
			Description: "context as null",
			JSON:        `{"error":"some error","exceptionId":"123","status":"error","context":null}`,
			ExpectedError: &Error{
				Message:     "some error",
				ExceptionID: "123",
				Context:     nil,
			},
			ExpectedContext: nil,
			ShouldFail:      false,
		},
		{
			Description: "context missing",
			JSON:        `{"error":"some error","exceptionId":"123","status":"error"}`,
			ExpectedError: &Error{
				Message:     "some error",
				ExceptionID: "123",
				Context:     nil,
			},
			ExpectedContext: nil,
			ShouldFail:      false,
		},
		{
			Description: "context as non-empty array",
			JSON:        `{"error":"some error","exceptionId":"123","status":"error","context":["item1","item2"]}`,
			ExpectedError: &Error{
				Message:     "some error",
				ExceptionID: "123",
				Context:     nil,
			},
			ExpectedContext: nil,
			ShouldFail:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Description, func(t *testing.T) {
			t.Parallel()

			var err Error
			unmarshalErr := json.Unmarshal([]byte(tc.JSON), &err)

			if tc.ShouldFail {
				assert.Error(t, unmarshalErr, tc.Description)
			} else {
				require.NoError(t, unmarshalErr, tc.Description)
				assert.Equal(t, tc.ExpectedError.Message, err.Message, tc.Description)
				assert.Equal(t, tc.ExpectedError.ExceptionID, err.ExceptionID, tc.Description)
				assert.Equal(t, tc.ExpectedContext, err.Context, tc.Description)
			}
		})
	}
}
