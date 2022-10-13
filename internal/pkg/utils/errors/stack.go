package errors

import (
	"runtime"
	"strings"
)

type StackTrace = []uintptr

type stackTracer interface {
	StackTrace() StackTrace
}

// nolint:errname
type withStack struct {
	err   error
	stack StackTrace
}

func WithStack(err error) error {
	return &withStack{err: err, stack: callers()}
}

func (e withStack) Error() string {
	return e.err.Error()
}

func (e withStack) Unwrap() error {
	return e.err
}

func (e withStack) StackTrace() StackTrace {
	return e.stack
}

// callers returns stack trace. Frames inside errors package itself are skipped.
func callers() StackTrace {
	const depth = 8
	const skip = 3 // first 3 frames are always from errors package

	rpc := make([]uintptr, depth)
	n := runtime.Callers(skip, rpc)
	rpc = rpc[0:n]

	// Skip frames from errors package
	var out []uintptr
	for _, pc := range rpc {
		pc = pc - 1
		fn := runtime.FuncForPC(pc)
		if !strings.Contains(fn.Name(), "utils/errors.") {
			out = append(out, pc)
		}
	}
	return out
}
