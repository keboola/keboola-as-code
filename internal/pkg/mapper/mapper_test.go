package mapper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMappers_ForEach_StopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEach(true, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, `error 1`, err.Error())
	assert.Equal(t, []string{`1`}, callOrder)
}

func TestMappers_ForEach_DontStopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEach(false, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, "- error 1\n- error 2\n- error 3\n- error 4\n- error 5", err.Error())
	assert.Equal(t, []string{`1`, `2`, `3`, `4`, `5`}, callOrder)
}

func TestMappers_ForEachReverse_StopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEachReverse(true, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, `error 5`, err.Error())
	assert.Equal(t, []string{`5`}, callOrder)
}

func TestMappers_ForEachReverse_DontStopOnFailure(t *testing.T) {
	t.Parallel()
	callOrder := make([]string, 0)
	mappers := Mappers{`1`, `2`, `3`, `4`, `5`}
	err := mappers.ForEachReverse(false, func(mapper interface{}) error {
		callOrder = append(callOrder, mapper.(string))
		return fmt.Errorf(`error %s`, mapper.(string))
	})
	assert.Error(t, err)
	assert.Equal(t, "- error 5\n- error 4\n- error 3\n- error 2\n- error 1", err.Error())
	assert.Equal(t, []string{`5`, `4`, `3`, `2`, `1`}, callOrder)
}
