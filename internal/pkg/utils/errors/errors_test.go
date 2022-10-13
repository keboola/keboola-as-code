package errors_test

import (
	"fmt"

	. "github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MultiErrorForTest() error {
	errWithStackTrace := WithStack(fmt.Errorf("error with debug trace"))

	e := NewMultiError()
	e.Append(fmt.Errorf(`error 1`))

	merged := NewMultiError()
	merged.Append(errWithStackTrace)
	merged.Append(
		fmt.Errorf(
			"wrapped2: %w",
			fmt.Errorf(
				"wrapped1: %w",
				WithStack(fmt.Errorf("error 2")),
			)),
	)

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

	sub.AppendWithPrefix(sub1, "sub1")
	sub.AppendWithPrefix(sub2, "sub2")
	sub.AppendWithPrefixf(sub3, "sub3 with %s", "format")
	sub.AppendNested(fmt.Errorf("sub4")).Append(
		New("1"),
		New("2"),
		New("3"),
	)

	e.Append(merged)
	e.AppendWithPrefix(sub, "my prefix")
	e.Append(fmt.Errorf("last error"))

	return e.ErrorOrNil()
}
