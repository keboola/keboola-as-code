package utils

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleError(t *testing.T) {
	t.Parallel()
	e := NewMultiError()
	e.Append(fmt.Errorf(`foo bar`))
	assert.Equal(t, `foo bar`, e.Error())
}

func TestMultiError(t *testing.T) {
	t.Parallel()
	e := NewMultiError()
	e.Append(fmt.Errorf(`12345`))

	merged := NewMultiError()
	merged.Append(fmt.Errorf("merged 1"))
	merged.Append(fmt.Errorf("merged 2"))

	sub := NewMultiError()
	sub.Append(fmt.Errorf(`abc`))
	sub.Append(fmt.Errorf(`def`))

	sub1 := NewMultiError()
	sub1.Append(fmt.Errorf("x"))
	sub1.Append(fmt.Errorf("y"))
	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("z"))
	sub3 := NewMultiError()
	sub3.Append(fmt.Errorf("this is a very long line from error message, it is printed on new line"))

	sub.AppendWithPrefix("sub1", sub1)
	sub.AppendWithPrefix("sub2", sub2)
	sub.AppendWithPrefix("sub3", sub3)

	e.Append(merged)
	e.AppendWithPrefix("my prefix", sub)
	e.Append(fmt.Errorf("last error"))

	expected := `
- 12345
- merged 1
- merged 2
- my prefix:
  - abc
  - def
  - sub1:
    - x
    - y
  - sub2: z
  - sub3:
    - this is a very long line from error message, it is printed on new line
- last error
`
	assert.Equal(t, strings.TrimSpace(expected), e.Error())
}

func TestMultiError_Flatten(t *testing.T) {
	t.Parallel()
	a := NewMultiError()
	a.Append(fmt.Errorf("A 1"))
	a.Append(fmt.Errorf("A 2"))

	b := NewMultiError()
	b.Append(fmt.Errorf("B 1"))
	b.Append(fmt.Errorf("B 2"))

	c := NewMultiError()
	c.Append(fmt.Errorf("C 1"))
	c.Append(fmt.Errorf("C 2"))

	merged := NewMultiError()
	merged.Append(a)
	merged.Append(b)
	merged.AppendWithPrefix("Prefix", c)
	assert.Len(t, merged.Errors, 5)

	expected := `
- A 1
- A 2
- B 1
- B 2
- Prefix:
  - C 1
  - C 2
`
	assert.Equal(t, strings.TrimSpace(expected), merged.Error())
}
