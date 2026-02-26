package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestNonRetryableError_Error(t *testing.T) {
	t.Parallel()

	baseErr := errors.New("test error message")
	nonRetryableErr := model.NewNonRetryableError(baseErr)

	assert.Equal(t, "test error message", nonRetryableErr.Error())
}

func TestNonRetryableError_Unwrap(t *testing.T) {
	t.Parallel()

	baseErr := errors.New("wrapped error")
	nonRetryableErr := model.NewNonRetryableError(baseErr)

	assert.Equal(t, baseErr, nonRetryableErr.Unwrap())
}

func TestNonRetryableError_ErrorsAs(t *testing.T) {
	t.Parallel()

	// Test that errors.As correctly identifies NonRetryableError through wrapping
	baseErr := errors.New("base error")
	nonRetryableErr := model.NewNonRetryableError(baseErr)
	wrappedErr := errors.Errorf("wrapped: %w", nonRetryableErr)

	var target *model.NonRetryableError
	assert.True(t, errors.As(wrappedErr, &target))
	assert.Equal(t, nonRetryableErr, target)
	assert.Equal(t, "base error", target.Err.Error())
}

func TestNonRetryableError_ErrorsAs_Negative(t *testing.T) {
	t.Parallel()

	// Test that a regular error is not identified as NonRetryableError
	regularErr := errors.New("regular error")

	var target *model.NonRetryableError
	assert.False(t, errors.As(regularErr, &target))
	assert.Nil(t, target)
}
