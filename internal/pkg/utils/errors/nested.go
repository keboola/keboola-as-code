package errors

type NestedError interface {
	Len() int
	Error() string
	Unwrap() error
	StackTrace() StackTrace
	MainError() error
	WrappedErrors() []error
	Append(errs ...error)
	AppendNested(err error) NestedError
	AppendWithPrefix(err error, prefix string)
	AppendWithPrefixf(err error, format string, a ...any)
}

type nestedErrorGetter interface {
	MainError() error
	WrappedErrors() []error
}

type nestedError struct {
	main      error
	subErrors MultiError
	trace     StackTrace
}

func (e *nestedError) Len() int {
	return e.subErrors.Len()
}

func (e *nestedError) Error() string {
	return Format(e)
}

func (e *nestedError) Unwrap() error {
	return append(chain{e.main}, e.subErrors.WrappedErrors()...)
}

func (e *nestedError) StackTrace() StackTrace {
	return e.trace
}

func (e *nestedError) MainError() error {
	return e.main
}

func (e *nestedError) WrappedErrors() []error {
	return e.subErrors.WrappedErrors()
}

func (e *nestedError) Append(errs ...error) {
	e.subErrors.Append(errs...)
}

func (e *nestedError) AppendNested(err error) NestedError {
	return e.subErrors.AppendNested(err)
}

func (e *nestedError) AppendWithPrefix(err error, prefix string) {
	e.subErrors.AppendWithPrefix(err, prefix)
}

func (e *nestedError) AppendWithPrefixf(err error, format string, a ...any) {
	e.subErrors.AppendWithPrefixf(err, format, a...)
}

func PrefixError(err error, prefix string) error {
	return NewNestedError(New(prefix), err)
}

func PrefixErrorf(err error, format string, a ...any) error {
	return NewNestedError(Errorf(format, a...), err)
}

func NewNestedError(main error, subErrs ...error) NestedError {
	if main == nil {
		panic("error cannot be nil")
	}

	subMultiError := NewMultiError()
	for _, err := range subErrs {
		if v, ok := err.(MultiError); ok { // nolint: errorlint
			subMultiError.Append(v.WrappedErrors()...)
		} else {
			subMultiError.Append(err)
		}
	}

	return &nestedError{main: main, subErrors: subMultiError, trace: callers()}
}
