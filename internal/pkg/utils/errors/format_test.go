package errors_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type multiErrsGetterError struct{}

func (e multiErrsGetterError) Error() string {
	return Format(e)
}

func (e multiErrsGetterError) Unwrap() error {
	return e.multiError()
}

func (e multiErrsGetterError) MainError() error {
	return New("main error")
}

func (e multiErrsGetterError) WrappedErrors() []error {
	return []error{e.multiError()}
}

func (e multiErrsGetterError) multiError() error {
	errs := NewMultiError()
	errs.Append(New("error 1"))
	errs.Append(New("error 2"))
	return errs
}

func TestSingleError_Format(t *testing.T) {
	t.Parallel()
	e := NewMultiError()
	e.Append(fmt.Errorf("foo bar"))
	assert.Equal(t, "foo bar", e.Error())
}

func TestSingleError_FormatWithStack(t *testing.T) {
	t.Parallel()
	e := NewMultiError()
	e.Append(fmt.Errorf("foo bar"))
	wildcards.Assert(t, "foo bar [%s/format_test.go:%s]", Format(e, FormatWithStack()))
}

func TestMultiError_Format(t *testing.T) {
	t.Parallel()
	expected := `
- error 1
- error with debug trace
- wrapped2: wrapped1: error 2
- my prefix:
  - abc
  - def
  - sub1:
    - x
    - y
  - sub2: z
  - sub3 with format:
    - this is a very long line from error message, it is printed on new line
  - sub4:
    - 1
    - 2
    - 3
- last error
`
	assert.Equal(t, strings.TrimSpace(expected), MultiErrorForTest().Error())
}

func TestMultiErrorGetter_Format(t *testing.T) {
	t.Parallel()
	expected := `
main error:
- error 1
- error 2
`
	assert.Equal(t, strings.TrimSpace(expected), (multiErrsGetterError{}).Error())
}

func TestMultiError_Format_WithToSentence(t *testing.T) {
	t.Parallel()
	expected := `
- Error 1.
- Error with debug trace.
- Wrapped2: wrapped1: error 2.
- My prefix:
  - Abc.
  - Def.
  - Sub1:
    - X.
    - Y.
  - Sub2: Z.
  - Sub3 with format:
    - This is a very long line from error message, it is printed on new line.
  - Sub4:
    - 1.
    - 2.
    - 3.
- Last error.
`
	assert.Equal(t, strings.TrimSpace(expected), Format(MultiErrorForTest(), FormatAsSentences()))
}

func TestMultiError_FormatWithUnwrap(t *testing.T) {
	t.Parallel()
	expected := `
- error 1
- error with debug trace
- wrapped2: wrapped1: error 2 (*fmt.wrapError):
  - wrapped1: error 2 (*fmt.wrapError):
    - error 2
- my prefix:
  - abc
  - def
  - sub1:
    - x
    - y
  - sub2: z
  - sub3 with format:
    - this is a very long line from error message, it is printed on new line
  - sub4:
    - 1
    - 2
    - 3
- last error
`
	wildcards.Assert(t, strings.TrimSpace(expected), Format(MultiErrorForTest(), FormatWithUnwrap()))
}

func TestMultiError_FormatWithStack(t *testing.T) {
	t.Parallel()
	expected := `
- error 1 [%s/errors_test.go:%d]
- error with debug trace [%s/errors_test.go:%d]
- wrapped2: wrapped1: error 2 [%s/errors_test.go:%d] (*fmt.wrapError):
  - wrapped1: error 2 [%s/errors_test.go:%d] (*fmt.wrapError):
    - error 2 [%s/errors_test.go:%d]
- my prefix [%s/errors_test.go:%d]:
  - abc [%s/errors_test.go:%d]
  - def [%s/errors_test.go:%d]
  - sub1 [%s/errors_test.go:%d]:
    - x [%s/errors_test.go:%d]
    - y [%s/errors_test.go:%d]
  - sub2 [%s/errors_test.go:%d]:
    - z [%s/errors_test.go:%d]
  - sub3 with format [%s/errors_test.go:%d]:
    - this is a very long line from error message, it is printed on new line [%s/errors_test.go:%d]
  - sub4 [%s/errors_test.go:%d]:
    - 1 [%s/errors_test.go:%d]
    - 2 [%s/errors_test.go:%d]
    - 3 [%s/errors_test.go:%d]
- last error [%s/errors_test.go:%d]
`
	wildcards.Assert(t, strings.TrimSpace(expected), Format(MultiErrorForTest(), FormatWithStack()))
}

func TestMultiError_FormatWithStack_WithToSentence(t *testing.T) {
	t.Parallel()
	expected := `
- Error 1. [%s/errors_test.go:%d]
- Error with debug trace. [%s/errors_test.go:%d]
- Wrapped2: wrapped1: error 2. [%s/errors_test.go:%d] (*fmt.wrapError):
  - Wrapped1: error 2. [%s/errors_test.go:%d] (*fmt.wrapError):
    - Error 2. [%s/errors_test.go:%d]
- My prefix. [%s/errors_test.go:%d]:
  - Abc. [%s/errors_test.go:%d]
  - Def. [%s/errors_test.go:%d]
  - Sub1. [%s/errors_test.go:%d]:
    - X. [%s/errors_test.go:%d]
    - Y. [%s/errors_test.go:%d]
  - Sub2. [%s/errors_test.go:%d]:
    - Z. [%s/errors_test.go:%d]
  - Sub3 with format. [%s/errors_test.go:%d]:
    - This is a very long line from error message, it is printed on new line. [%s/errors_test.go:%d]
  - Sub4. [%s/errors_test.go:%d]:
    - 1. [%s/errors_test.go:%d]
    - 2. [%s/errors_test.go:%d]
    - 3. [%s/errors_test.go:%d]
- Last error. [%s/errors_test.go:%d]
`
	wildcards.Assert(t, strings.TrimSpace(expected), Format(MultiErrorForTest(), FormatWithStack(), FormatAsSentences()))
}

func TestMultiError_CustomMessageFormatter_Format(t *testing.T) {
	t.Parallel()

	// Custom function to modify message
	f := NewFormatter().
		WithPrefixFormatter(func(prefix string) string {
			return prefix + " --->"
		}).
		WithMessageFormatter(func(msg string, _ StackTrace, _ FormatConfig) string {
			return fmt.Sprintf("<<< %s >>>", msg)
		})

	expected := `
- <<< error 1 >>>
- <<< error with debug trace >>>
- <<< wrapped2: wrapped1: error 2 >>>
- <<< my prefix >>> --->
  - <<< abc >>>
  - <<< def >>>
  - <<< sub1 >>> --->
    - <<< x >>>
    - <<< y >>>
  - <<< sub2 >>> ---> <<< z >>>
  - <<< sub3 with format >>> --->
    - <<< this is a very long line from error message, it is printed on new line >>>
  - <<< sub4 >>> --->
    - <<< 1 >>>
    - <<< 2 >>>
    - <<< 3 >>>
- <<< last error >>>
`
	assert.Equal(t, strings.TrimSpace(expected), f.Format(MultiErrorForTest()))
}

func TestMultiError_CustomMessageFormatter_FormatWithStack(t *testing.T) {
	t.Parallel()

	// Custom function to modify message
	f := NewFormatter().
		WithPrefixFormatter(func(prefix string) string {
			return prefix + " --->"
		}).
		WithMessageFormatter(func(msg string, _ StackTrace, _ FormatConfig) string {
			return fmt.Sprintf("| %s |", msg)
		})

	expected := `
- | error 1 |
- | error with debug trace |
- | wrapped2: wrapped1: error 2 | (*fmt.wrapError) --->
  - | wrapped1: error 2 | (*fmt.wrapError) --->
    - | error 2 |
- | my prefix | --->
  - | abc |
  - | def |
  - | sub1 | --->
    - | x |
    - | y |
  - | sub2 | ---> | z |
  - | sub3 with format | --->
    - | this is a very long line from error message, it is printed on new line |
  - | sub4 | --->
    - | 1 |
    - | 2 |
    - | 3 |
- | last error |
`
	assert.Equal(t, strings.TrimSpace(expected), f.Format(MultiErrorForTest(), FormatWithStack()))
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
	merged.AppendWithPrefix(c, "Prefix")
	assert.Equal(t, 5, merged.Len())

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

func TestWrap_Format(t *testing.T) {
	t.Parallel()

	original := ErrorForTest()
	err := NewMultiError()
	err.Append(original)
	err.Append(Wrap(original, "different message"))
	expected := `
- some error
- different message
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestWrap_FormatWithStack(t *testing.T) {
	t.Parallel()

	original := ErrorForTest()
	err := NewMultiError()
	err.Append(original)
	err.Append(Wrap(original, "different message"))
	expected := `
- some error [%s/errors_test.go:%d]
- different message [%s/format_test.go:%s] (*errors.wrappedError):
  - some error [%s/errors_test.go:%d]
`
	wildcards.Assert(t, strings.TrimSpace(expected), Format(err, FormatWithStack()))
}

func TestWrapf_Format(t *testing.T) {
	t.Parallel()

	original := ErrorForTest()
	err := NewMultiError()
	err.Append(original)
	err.Append(Wrapf(original, "different %s", "message"))
	expected := `
- some error
- different message
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestWrapf_FormatWithStack(t *testing.T) {
	t.Parallel()

	original := ErrorForTest()
	err := NewMultiError()
	err.Append(original)
	err.Append(Wrapf(original, "different %s", "message"))
	expected := `
- some error [%s/errors_test.go:%d]
- different message [%s/format_test.go:%s] (*errors.wrappedError):
  - some error [%s/errors_test.go:%d]
`
	wildcards.Assert(t, strings.TrimSpace(expected), Format(err, FormatWithStack()))
}

func TestNestedError_Format_1(t *testing.T) {
	t.Parallel()

	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("a"))
	sub2.Append(fmt.Errorf("b"))
	sub2.Append(fmt.Errorf("c"))

	sub1 := NewNestedError(fmt.Errorf("reason"), sub2)
	err := NewNestedError(fmt.Errorf("error"), sub1)
	expected := `
error:
- reason:
  - a
  - b
  - c
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestNestedError_Format_2(t *testing.T) {
	t.Parallel()

	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("a lorem impsum"))
	sub2.Append(fmt.Errorf("b lorem impsum"))
	sub2.Append(fmt.Errorf("c lorem impsum"))

	sub1 := NewNestedError(fmt.Errorf("reason"), sub2)
	err1 := PrefixError(sub1, "error1")
	err2 := PrefixError(err1, "error2")
	expected := `
error2:
- error1:
  - reason:
    - a lorem impsum
    - b lorem impsum
    - c lorem impsum
`
	assert.Equal(t, strings.TrimSpace(expected), err2.Error())
}

func TestNestedError_Format_3(t *testing.T) {
	t.Parallel()

	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("lorem ipsum"))
	sub1 := NewNestedError(fmt.Errorf("reason"), sub2)
	err := NewNestedError(fmt.Errorf("error"), sub1)
	expected := `
error: reason: lorem ipsum
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestNestedError_Format_4(t *testing.T) {
	t.Parallel()

	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"))
	sub1 := NewNestedError(fmt.Errorf("reason"), sub2)
	err := NewNestedError(fmt.Errorf("error"), sub1)
	expected := `
error:
- reason:
  - lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

type customError struct {
	error
}

func (e customError) WriteError(w Writer, level int, _ StackTrace) {
	w.WritePrefix(level, fmt.Sprintf("this is a custom error message (%s)", e.Error()), nil)
	w.WriteNewLine()

	w.WriteBullet(level + 1)
	w.Write("foo")
	w.WriteNewLine()

	w.WriteBullet(level + 1)
	w.Write("bar")
}

func TestCustom_WriteError(t *testing.T) {
	t.Parallel()

	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("lorem ipsum"))
	sub2.Append(customError{New("underlying error")})
	sub1 := NewNestedError(fmt.Errorf("reason"), sub2)
	err := NewNestedError(fmt.Errorf("error"), sub1)
	expected := `
error:
- reason:
  - lorem ipsum
  - this is a custom error message (underlying error):
    - foo
    - bar
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestCustomMultiLineError_1(t *testing.T) {
	t.Parallel()

	sub2 := NewMultiError()
	sub2.Append(fmt.Errorf("lorem ipsum"))
	sub2.Append(New("* A\n* B\n* C"))
	sub1 := NewNestedError(fmt.Errorf("reason"), sub2)
	err := NewNestedError(fmt.Errorf("error"), sub1)
	expected := `
error:
- reason:
  - lorem ipsum
  - * A
    * B
    * C
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestCustomMultiLineError_2(t *testing.T) {
	t.Parallel()

	sub := NewMultiError()
	sub.Append(fmt.Errorf("lorem ipsum"))
	sub.Append(New("* A\n* B\n* C"))
	err := NewNestedError(fmt.Errorf("error"), sub)
	expected := `
error:
- lorem ipsum
- * A
  * B
  * C
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestCustomMultiLineError_3(t *testing.T) {
	t.Parallel()
	err := NewNestedError(fmt.Errorf("error"), New("* A\n* B\n* C"))
	expected := `
error:
- * A
  * B
  * C
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}
