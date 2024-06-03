package errors_test

import (
	"fmt"
	"regexp"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func ExampleNew() {
	fmt.Println(errors.New("some error"))
	// output:
	// some error
}

func ExampleErrorf() {
	err := errors.Errorf("enhanced error message: %w", errors.New("original error"))
	fmt.Println(err)
	// output:
	// enhanced error message: original error
}

func ExampleWrapf() {
	err := errors.Wrapf(errors.New("original error"), "new error %s", "message")
	fmt.Println(errors.Format(err, errors.FormatWithUnwrap()))
	// output:
	// new error message (*errors.wrappedError):
	//- original error
}

func ExampleWrap() {
	err := errors.Wrap(errors.New("original error"), "new error message")
	fmt.Println(errors.Format(err, errors.FormatWithUnwrap()))
	// output:
	// new error message (*errors.wrappedError):
	//- original error
}

func ExampleWithStack() {
	originalErr := errors.New("original error")
	err := errors.WithStack(originalErr)
	re := regexp.MustCompile(`\[.*/internal`)
	fmt.Println(string(re.ReplaceAll([]byte(errors.Format(err, errors.FormatWithStack())), []byte("["))))
	// output:
	// original error [/pkg/utils/errors/example_test.go:40]
}

func ExampleFormatWithStack() {
	originalErr := errors.New("original error")
	wrappedErr := errors.Wrapf(originalErr, "new error %s", "message")
	fmt.Println("Standard output:")
	fmt.Println(errors.Format(wrappedErr))
	fmt.Println()
	fmt.Println("FormatWithStack:")
	re := regexp.MustCompile(`\[.*/internal`)
	fmt.Println(string(re.ReplaceAll([]byte(errors.Format(wrappedErr, errors.FormatWithStack())), []byte("["))))
	// output:
	// Standard output:
	// new error message
	//
	// FormatWithStack:
	// new error message [/pkg/utils/errors/example_test.go:50] (*errors.wrappedError):
	// - original error [/pkg/utils/errors/example_test.go:49]
}

func ExampleFormatWithUnwrap() {
	originalErr := errors.New("original error")
	wrappedErr := errors.Wrapf(originalErr, "new error %s", "message")
	fmt.Println("Standard output:")
	fmt.Println(errors.Format(wrappedErr))
	fmt.Println()
	fmt.Println("FormatWithUnwrap:")
	fmt.Println(errors.Format(wrappedErr, errors.FormatWithUnwrap()))
	// output:
	// Standard output:
	// new error message
	//
	// FormatWithUnwrap:
	// new error message (*errors.wrappedError):
	// - original error
}

func ExampleFormatAsSentences() {
	err := errors.NewNestedError(
		errors.New("foo"),
		errors.New("bar1"),
		errors.New("bar2"),
	)
	fmt.Println("Standard output:")
	fmt.Println(errors.Format(err))
	fmt.Println()
	fmt.Println("FormatAsSentences:")
	fmt.Println(errors.Format(err, errors.FormatAsSentences()))
	// output:
	// Standard output:
	// foo:
	// - bar1
	// - bar2
	//
	// FormatAsSentences:
	// Foo:
	// - Bar1.
	// - Bar2.
}

func Example_format() {
	errs := errors.NewMultiError()
	errs.Append(errors.New("foo 1"))
	errs.Append(errors.New("foo 2"))
	errs.Append(errors.Wrapf(errors.New("original error"), "new error %s", "message"))

	fmt.Println("Standard output:")
	fmt.Println(errors.Format(errs.ErrorOrNil()))
	fmt.Println()
	fmt.Println("FormatWithUnwrap:")
	fmt.Println(errors.Format(errs.ErrorOrNil(), errors.FormatWithUnwrap()))
	fmt.Println()
	fmt.Println("FormatAsSentences:")
	fmt.Println(errors.Format(errs.ErrorOrNil(), errors.FormatAsSentences()))
	fmt.Println()
	fmt.Println("FormatWithUnwrap, FormatAsSentences:")
	fmt.Println(errors.Format(errs.ErrorOrNil(), errors.FormatWithUnwrap(), errors.FormatAsSentences()))
	// output:
	// Standard output:
	// - foo 1
	// - foo 2
	// - new error message
	//
	// FormatWithUnwrap:
	// - foo 1
	// - foo 2
	// - new error message (*errors.wrappedError):
	//   - original error
	//
	// FormatAsSentences:
	// - Foo 1.
	// - Foo 2.
	// - New error message.
	//
	// FormatWithUnwrap, FormatAsSentences:
	// - Foo 1.
	// - Foo 2.
	// - New error message. (*errors.wrappedError):
	//   - Original error.
}

func Example_multiError() {
	errs := errors.NewMultiError()
	errs.Append(errors.New("foo 1"))
	errs.Append(errors.New("foo 2"))

	sub := errs.AppendNested(errors.New("some sub error 1"))
	sub.Append(errors.New("foo 3"))
	sub.Append(errors.New("foo 4"))

	errs.AppendWithPrefixf(errors.New("nested error"), "some %s", "prefix")

	errs.Append(errors.NewNestedError(
		errors.New("some sub error 2"),
		errors.New("foo 5"),
		errors.New("foo 6"),
	))

	// return errs.ErrorOrNil()

	fmt.Println("Standard output:")
	fmt.Println(errors.Format(errs))
	fmt.Println()
	fmt.Println("FormatAsSentences:")
	fmt.Println(errors.Format(errs, errors.FormatAsSentences()))
	// output:
	// Standard output:
	// - foo 1
	// - foo 2
	// - some sub error 1:
	//   - foo 3
	//   - foo 4
	// - some prefix: nested error
	// - some sub error 2:
	//   - foo 5
	//   - foo 6
	//
	// FormatAsSentences:
	// - Foo 1.
	// - Foo 2.
	// - Some sub error 1:
	//   - Foo 3.
	//   - Foo 4.
	// - Some prefix: Nested error.
	// - Some sub error 2:
	//   - Foo 5.
	//   - Foo 6.
}
