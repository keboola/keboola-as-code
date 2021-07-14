package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestSingleError(t *testing.T) {
	e := NewMultiError()
	e.Append(fmt.Errorf(`foo bar`))
	assert.Equal(t, `foo bar`, e.Error())
}

func TestMultiError(t *testing.T) {
	e := NewMultiError()
	e.Append(fmt.Errorf(`12345`))
	e.AppendRaw(`45678`)

	merged := NewMultiError()
	merged.Append(fmt.Errorf("merged 1"))
	merged.Append(fmt.Errorf("merged 2"))

	sub := NewMultiError()
	sub.Append(fmt.Errorf(`abc`))
	sub.Append(fmt.Errorf(`def`))
	sub.AppendRaw(`xyz`)

	sub1 := NewMultiError()
	sub1.Append(fmt.Errorf("x"))
	sub1.Append(fmt.Errorf("y"))
	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("z"))

	sub.AppendWithPrefix("sub1", sub1)
	sub.AppendWithPrefix("sub1", sub2)

	e.Append(merged)
	e.AppendWithPrefix("my prefix", sub)
	e.Append(fmt.Errorf("last error"))

	expected := `
- 12345
45678
- merged 1
- merged 2
- my prefix:
	- abc
	- def
	- xyz
	- sub1:
		- x
		- y
	- sub1:
		- z
- last error
`
	assert.Equal(t, strings.TrimSpace(expected), e.Error())
}
