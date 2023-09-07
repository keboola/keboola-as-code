package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestRetryable_Validation(t *testing.T) {
	cases := []struct {
		Name          string
		ExpectedError string
		Value         Retryable
	}{
		{
			Name:  "empty",
			Value: Retryable{},
		},
		{
			Name:          "attempt=0, unexpected fields",
			ExpectedError: "- \"retryReason\" should not be set\n- \"failedAt\" should not be set\n- \"retryAfter\" should not be set",
			Value: Retryable{
				RetryAttempt: 0,
				RetryReason:  "foo",
				FailedAt:     ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				RetryAfter:   ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
			},
		},
		{
			Name:          "attempt=1, missing fields",
			ExpectedError: "- \"retryReason\" is a required field\n- \"failedAt\" is a required field\n- \"retryAfter\" is a required field",
			Value: Retryable{
				RetryAttempt: 1,
			},
		},
		{
			Name: "attempt=1, ok",
			Value: Retryable{
				RetryAttempt: 1,
				RetryReason:  "foo",
				FailedAt:     ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
				RetryAfter:   ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
			},
		},
		{
			Name:          "retry after before failed at",
			ExpectedError: `"retryAfter" must be greater than or equal to FailedAt`,
			Value: Retryable{
				RetryAttempt: 1,
				RetryReason:  "foo",
				FailedAt:     ptr(utctime.MustParse("2006-01-02T17:04:05.000Z")),
				RetryAfter:   ptr(utctime.MustParse("2006-01-02T15:04:05.000Z")),
			},
		},
	}

	// Run test cases
	ctx := context.Background()
	val := validator.New()
	for _, tc := range cases {
		err := val.Validate(ctx, tc.Value)
		if tc.ExpectedError == "" {
			assert.NoError(t, err, tc.Name)
		} else {
			if assert.Error(t, err, tc.Name) {
				assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
			}
		}
	}
}
