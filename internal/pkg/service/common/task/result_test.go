package task

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestOkResult(t *testing.T) {
	t.Parallel()
	result := OkResult("task succeeded")
	assert.Equal(t, Result{result: "task succeeded"}, result)
	assert.Equal(t, "task succeeded", result.Result())
	assert.False(t, result.IsErr())
	assert.Nil(t, result.Err())

	// WithResult
	result = result.WithResult("new message")
	assert.Equal(t, Result{result: "new message"}, result)
	assert.Equal(t, "new message", result.Result())
	assert.False(t, result.IsErr())
	assert.Nil(t, result.Err())

	// WithErr
	assert.PanicsWithError(t, `result type is "ok", not "error", it cannot be modified`, func() {
		result.WithErr(errors.New("error msg"))
	})
}

func TestOkResult_WithOutput(t *testing.T) {
	t.Parallel()
	result := OkResult("task succeeded").WithOutput("key1", 123).WithOutput("key2", "foo")
	assert.Equal(t, Result{
		result: "task succeeded",
		outputs: map[string]any{
			"key1": 123,
			"key2": "foo",
		},
	}, result)
	assert.Equal(t, "task succeeded", result.Result())
	assert.False(t, result.IsErr())
	assert.Nil(t, result.Err())

	// WithResult
	result = result.WithResult("new message")
	assert.Equal(t, Result{
		result: "new message",
		outputs: map[string]any{
			"key1": 123,
			"key2": "foo",
		},
	}, result)
	assert.Equal(t, "new message", result.Result())
	assert.False(t, result.IsErr())
	assert.Nil(t, result.Err())

	// WithErr
	assert.PanicsWithError(t, `result type is "ok", not "error", it cannot be modified`, func() {
		result.WithErr(errors.New("error msg"))
	})
}

func TestErrResult(t *testing.T) {
	t.Parallel()
	err := errors.New("task failed")
	result := ErrResult(err)
	assert.Equal(t, Result{error: err}, result)
	assert.True(t, result.IsErr())
	assert.Equal(t, err, result.Err())

	// WithErr
	err = errors.New("new error")
	result = result.WithErr(err)
	assert.Equal(t, Result{error: err}, result)
	assert.True(t, result.IsErr())
	assert.Equal(t, err, result.Err())

	// WithResult
	assert.PanicsWithError(t, `result type is "error", not "ok", it cannot be modified`, func() {
		result.WithResult("msg")
	})
}

func TestErrResult_WithOutput(t *testing.T) {
	t.Parallel()
	err := errors.New("task failed")
	result := ErrResult(err).WithOutput("key1", 123).WithOutput("key2", "foo")
	assert.Equal(t, Result{
		error: err,
		outputs: map[string]any{
			"key1": 123,
			"key2": "foo",
		},
	}, result)
	assert.Equal(t, "", result.Result())
	assert.True(t, result.IsErr())
	assert.Equal(t, err, result.Err())

	// WithErr
	err = errors.New("new error")
	result = result.WithErr(err)
	assert.Equal(t, Result{
		error: err,
		outputs: map[string]any{
			"key1": 123,
			"key2": "foo",
		},
	}, result)
	assert.Equal(t, "", result.Result())
	assert.True(t, result.IsErr())
	assert.Equal(t, err, result.Err())

	// WithResult
	assert.PanicsWithError(t, `result type is "error", not "ok", it cannot be modified`, func() {
		result.WithResult("msg")
	})
}
