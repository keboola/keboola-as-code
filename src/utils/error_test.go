package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestError(t *testing.T) {
	e := &Error{}
	assert.Equal(t, 0, e.Len())
	assert.Equal(t, "", e.Error())

	e.Add(fmt.Errorf("foo"))
	e.Add(fmt.Errorf("bar"))
	assert.Equal(t, 2, e.Len())
	assert.Equal(t, "- foo\n- bar\n", e.Error())
}
